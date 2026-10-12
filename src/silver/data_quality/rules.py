from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from pyspark.sql import Column

from silver.data_quality.checks import CHECKS

HARD = "H"   # row goes to quarantine
SOFT = "S"   # row stays in trusted, with the rule ID recorded as a warning
PARSED = "parsed"


@dataclass(frozen=True)
class Rule:
    rule_id: str                      # e.g. "CUS-001", unique across the project
    severity: str                     # HARD or SOFT
    check: str                        # key in CHECKS
    columns: tuple[str, ...]
    params: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.severity not in (HARD, SOFT):
            raise ValueError(f"{self.rule_id}: severity must be 'H' or 'S', got {self.severity!r}")
        if self.check not in CHECKS:
            raise ValueError(f"{self.rule_id}: unknown check {self.check!r}; known: {sorted(CHECKS)}")
        if not self.columns:
            raise ValueError(f"{self.rule_id}: no columns given")

    @property
    def is_hard(self) -> bool:
        return self.severity == HARD

    def passes(self) -> Column:
        """Boolean Column: True where the row passes this rule."""
        return CHECKS[self.check](self.columns, self.params)


def raw_columns(rules: Iterable[Rule]) -> tuple[str, ...]:
    """Standardised columns whose original value the parsed rules need, in first-seen order."""
    return tuple(dict.fromkeys(c for rule in rules if rule.check == PARSED for c in rule.columns))
