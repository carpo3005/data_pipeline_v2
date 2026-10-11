import pytest
from pyspark.sql import functions as F

from pipeline.errors import DataValidationError
from silver.standardisation.column_rules import (
    StandardisationConfig, apply_column_rules, build_rule_registry,
)

NULL_TOKENS = ("", "NULL", "N/A")


@pytest.fixture
def sample(spark):
    return spark.createDataFrame(
        [
            ("mIxEd CaSe", "yes", "abc", "hello world"),
            ("N/A", "maybe", None, "  spaced  "),
            (None, "N", "xyz", None),
        ],
        "name string, flag string, code string, state string",
    )


@pytest.fixture
def registry():
    return build_rule_registry(StandardisationConfig(null_tokens=NULL_TOKENS))


def test_config_from_dict_converts_lists_and_rejects_unknown_keys():
    config = StandardisationConfig.from_dict({"boolean_true": ["Y", "YES"]})
    assert config.boolean_true == ("Y", "YES")
    with pytest.raises(ValueError, match="Unknown config keys"):
        StandardisationConfig.from_dict({"boolean_treu": ["Y"]})


def test_registry_has_generic_rules(registry):
    assert {"string", "title", "upper", "lower", "boolean"} <= set(registry)


def test_extra_rules_can_add_and_replace(sample):
    registry = build_rule_registry(
        StandardisationConfig(null_tokens=NULL_TOKENS),
        extra_rules={"upper": lambda col: F.lit("replaced")},
    )
    result = apply_column_rules(sample, {"code": "upper"}, registry, NULL_TOKENS)
    assert {r.code for r in result.df.collect()} == {"replaced"}


def test_rules_standardise_each_column(sample, registry):
    result = apply_column_rules(
        sample,
        {"name": "title", "flag": "boolean", "code": "upper"},
        registry,
        NULL_TOKENS,
    )
    rows = result.df.select("name", "flag", "code").collect()
    assert [tuple(r) for r in rows] == [
        ("Mixed Case", True, "ABC"),
        (None, None, None),
        (None, False, "XYZ"),
    ]


def test_invalid_values_are_counted_not_raised(sample, registry):
    result = apply_column_rules(sample, {"flag": "boolean"}, registry, NULL_TOKENS)
    # "maybe" is unrecognised, so it becomes NULL and is counted; "N/A" is a null token and is not
    assert result.invalid_counts == {"invalid_flag_count": 1, "invalid_value_count": 1}


def test_unlisted_columns_pass_through(sample, registry):
    result = apply_column_rules(sample, {"name": "upper"}, registry, NULL_TOKENS)
    assert result.df.columns == sample.columns
    assert result.df.select("state").collect() == sample.select("state").collect()


def test_derived_column_reads_raw_source(sample, registry):
    result = apply_column_rules(
        sample,
        {"name": "lower"},
        registry,
        NULL_TOKENS,
        derived={"state_upper": ("state", "upper")},
    )
    assert "state_upper" in result.df.columns
    assert result.df.select("state_upper").collect()[0][0] == "HELLO WORLD"


def test_unregistered_rule_raises(sample, registry):
    with pytest.raises(ValueError, match="Rules not registered: \\['phone'\\]"):
        apply_column_rules(sample, {"name": "phone"}, registry, NULL_TOKENS)


def test_missing_column_raises(sample, registry):
    with pytest.raises(DataValidationError, match="Missing columns: nope"):
        apply_column_rules(sample, {"nope": "upper"}, registry, NULL_TOKENS)
