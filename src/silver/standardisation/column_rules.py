from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields

from pyspark.sql import Column, DataFrame, functions as F

from pipeline.validation import assert_columns_exist
from silver.standardisation.boolean import assert_disjoint, normalise_boolean
from silver.standardisation.country import normalise_country
from silver.standardisation.dates import parse_date
from silver.standardisation.phone import normalise_phone
from silver.standardisation.subdivision import map_uk_subdivision
from silver.standardisation.result import StandardisationResult, count_invalid
from silver.standardisation.string_cleaning import apply_case, clean_string

RuleFn = Callable[[Column], Column]
Target = tuple[str, str, str]  # (output column, source column, rule name)


@dataclass(frozen=True)
class StandardisationConfig:
    """Per-pipeline settings. Defaults are generic; locale data defaults to empty and is supplied by the pipeline."""
    null_tokens: tuple[str, ...] = ("", "NULL", "null", "N/A", "NA", "None")
    boolean_true: tuple[str, ...] = ("Y", "YES", "TRUE", "T", "1")
    boolean_false: tuple[str, ...] = ("N", "NO", "FALSE", "F", "0")
    date_formats: tuple[str, ...] = ("yyyy-MM-dd", "dd-MM-yyyy", "yyyy/MM/dd", "dd/MM/yyyy")
    country_aliases: Mapping[str, str] = field(default_factory=dict)
    phone: Mapping[str, object] = field(default_factory=dict)
    uk_subdivision_map: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping) -> "StandardisationConfig":
        """Build from a dict (e.g. parsed JSON). Lists become tuples; unknown keys raise."""
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"Unknown config keys: {sorted(unknown)}")
        return cls(**{k: tuple(v) if isinstance(v, list) else v for k, v in data.items()})


def build_rule_registry(
    config: StandardisationConfig,
    extra_rules: Mapping[str, RuleFn] | None = None,
) -> dict[str, RuleFn]:
    """Map rule name -> function(Column) -> Column. extra_rules can add or replace rules."""
    assert_disjoint(config.boolean_true, config.boolean_false)

    def cleaned(col: Column) -> Column:
        return clean_string(col, config.null_tokens)

    registry: dict[str, RuleFn] = {
        "string": cleaned,
        "title": lambda col: apply_case(cleaned(col), "title"),
        "upper": lambda col: apply_case(cleaned(col), "upper"),
        "lower": lambda col: apply_case(cleaned(col), "lower"),
        "boolean": lambda col: normalise_boolean(cleaned(col), config.boolean_true, config.boolean_false),
    }
    registry["date"] = lambda col: parse_date(cleaned(col), config.date_formats)
    if config.phone:
        registry["phone"] = _phone_rule(cleaned, config.phone)
    if config.country_aliases:
        registry["country"] = lambda col: normalise_country(cleaned(col), config.country_aliases)
    if config.uk_subdivision_map:
        registry["uk_subdivision"] = lambda col: map_uk_subdivision(cleaned(col), config.uk_subdivision_map)
    registry.update(extra_rules or {})
    return registry


def _phone_rule(cleaned: Callable[[Column], Column], phone: Mapping[str, object]) -> RuleFn:
    missing = {"country_code", "trunk_prefix", "national_lengths"} - set(phone)
    if missing:
        raise ValueError(f"phone config is missing {sorted(missing)}")
    lengths = tuple(phone["national_lengths"])
    return lambda col: normalise_phone(cleaned(col), phone["country_code"], phone["trunk_prefix"], lengths)


def apply_column_rules(
    df: DataFrame,
    rules: Mapping[str, str],
    registry: Mapping[str, RuleFn],
    null_tokens: tuple[str, ...],
    derived: Mapping[str, tuple[str, str]] | None = None,
    count_invalid_values: bool = True,
) -> StandardisationResult:
    """Apply one rule per column in a single projection.

    derived maps new_column -> (source_column, rule); it reads the raw source column,
    so it does not depend on the order of the other rules. Unlisted columns pass through.
    """
    derived = dict(derived or {})
    _check_rules(df, rules, derived, registry)
    outputs, checks = _build_outputs(_targets(rules, derived), registry, null_tokens)
    counts = count_invalid(df, checks) if count_invalid_values else {}
    return StandardisationResult(df=df.withColumns(outputs), invalid_counts=counts)


def _targets(rules: Mapping[str, str], derived: Mapping[str, tuple[str, str]]) -> list[Target]:
    direct = [(column, column, rule) for column, rule in rules.items()]
    computed = [(new, source, rule) for new, (source, rule) in derived.items()]
    return direct + computed


def _check_rules(
    df: DataFrame,
    rules: Mapping[str, str],
    derived: Mapping[str, tuple[str, str]],
    registry: Mapping[str, RuleFn],
) -> None:
    rule_names = {rule for _, _, rule in _targets(rules, derived)}
    unregistered = rule_names - set(registry)
    if unregistered:
        raise ValueError(f"Rules not registered: {sorted(unregistered)}; registered: {sorted(registry)}")
    assert_columns_exist(df, [source for _, source, _ in _targets(rules, derived)])


def _build_outputs(
    targets: list[Target],
    registry: Mapping[str, RuleFn],
    null_tokens: tuple[str, ...],
) -> tuple[dict[str, Column], dict[str, tuple[Column, Column]]]:
    outputs: dict[str, Column] = {}
    checks: dict[str, tuple[Column, Column]] = {}
    for name, source, rule in targets:
        source_col = F.col(source)
        outputs[name] = registry[rule](source_col)
        checks[name] = (clean_string(source_col, null_tokens), outputs[name])
    return outputs, checks
