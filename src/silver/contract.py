from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

_ALLOWED_KEYS = frozenset({"business_key", "cast_rules"})


@dataclass(frozen=True)
class CastRule:
    column_name: str
    data_type: str  # DDL string, e.g. "int" or "decimal(10,2)"


@dataclass(frozen=True)
class EntityContract:
    entity: str
    business_key: tuple[str, ...]
    cast_rules: tuple[CastRule, ...]
    column_rules: Mapping[str, str]  # column -> registered rule name
    source_table_fqn: str
    target_table_fqn: str

    @classmethod
    def from_dict(
        cls,
        entity: str,
        contract: Mapping,
        column_rules: Mapping[str, str],
        source_table_fqn: str,
        target_table_fqn: str,
        valid_rules: Iterable[str] | None = None,
    ) -> EntityContract:
        """Build a contract from {"business_key": ..., "cast_rules": {column: DDL type}}."""
        _check_known_keys(entity, contract)
        cast_rules = _parse_cast_rules(contract)
        _check_no_overlap(entity, cast_rules, column_rules)
        if valid_rules is not None:
            _check_registered(entity, column_rules, valid_rules)
        return cls(
            entity=entity,
            business_key=_parse_business_key(entity, contract),
            cast_rules=cast_rules,
            column_rules=dict(column_rules),
            source_table_fqn=source_table_fqn,
            target_table_fqn=target_table_fqn,
        )

    @classmethod
    def from_json(
        cls,
        entity: str,
        text: str,
        column_rules: Mapping[str, str],
        source_table_fqn: str,
        target_table_fqn: str,
        valid_rules: Iterable[str] | None = None,
    ) -> EntityContract:
        """Same as from_dict, reading the contract from a JSON string."""
        return cls.from_dict(
            entity, json.loads(text), column_rules, source_table_fqn, target_table_fqn, valid_rules
        )


def _check_known_keys(entity: str, contract: Mapping) -> None:
    if "business_key" not in contract:
        raise ValueError(f"Entity '{entity}': missing 'business_key'")
    unknown = set(contract) - _ALLOWED_KEYS
    if unknown:
        raise ValueError(f"Entity '{entity}': unknown contract keys {sorted(unknown)}")


def _parse_business_key(entity: str, contract: Mapping) -> tuple[str, ...]:
    raw = contract["business_key"]
    key = (raw,) if isinstance(raw, str) else tuple(raw)
    if not key:
        raise ValueError(f"Entity '{entity}': business_key is empty")
    return key


def _parse_cast_rules(contract: Mapping) -> tuple[CastRule, ...]:
    return tuple(
        CastRule(column_name=column, data_type=data_type)
        for column, data_type in contract.get("cast_rules", {}).items()
    )


def _check_no_overlap(entity: str, cast_rules: tuple[CastRule, ...], column_rules: Mapping[str, str]) -> None:
    overlap = {rule.column_name for rule in cast_rules} & set(column_rules)
    if overlap:
        raise ValueError(f"Entity '{entity}': columns in both cast_rules and column_rules: {sorted(overlap)}")


def _check_registered(entity: str, column_rules: Mapping[str, str], valid_rules: Iterable[str]) -> None:
    unregistered = set(column_rules.values()) - set(valid_rules)
    if unregistered:
        raise ValueError(f"Entity '{entity}': rules not registered: {sorted(unregistered)}")
