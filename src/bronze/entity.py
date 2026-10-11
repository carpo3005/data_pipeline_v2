from dataclasses import dataclass, field

from pyspark.sql.types import StructType


@dataclass(frozen=True)
class EntityConfig:
    """Everything bronze needs to ingest one source entity."""
    name: str
    source_path: str
    schema: StructType
    target_table: str                 # build with pipeline.catalog.get_table_fqn
    merge_keys: tuple[str, ...]       # business keys, or the record hash column for exact-duplicate dedupe
    read_options: dict[str, str] = field(default_factory=dict)
    allow_duplicates: bool = True
    allow_hash_conflicts: bool = True
