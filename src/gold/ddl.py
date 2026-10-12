import re
from string import Template

from pyspark.sql import SparkSession

from pipeline.session import is_databricks

_INFORMATIONAL_CONSTRAINT = re.compile(r"^\s*CONSTRAINT\s+\w+\s+(PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE)\b", re.IGNORECASE)
_CREATE_OR_REPLACE = re.compile(r"^\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+(\S+)", re.IGNORECASE)
_INFORMATIONAL_ALTER = re.compile(
    r"^\s*ALTER\s+TABLE\b.*\bADD\s+CONSTRAINT\s+\w+\s+(UNIQUE|PRIMARY\s+KEY|FOREIGN\s+KEY)\b",
    re.IGNORECASE | re.DOTALL,
)


def split_statements(script: str) -> list[str]:
    """Split a script on ';' after removing -- comments. Comment-only pieces are dropped."""
    without_comments = re.sub(r"--[^\n]*", "", script)
    return [piece.strip() for piece in without_comments.split(";") if piece.strip()]


def is_informational_alter(statement: str) -> bool:
    return bool(_INFORMATIONAL_ALTER.match(statement))


def strip_informational_constraints(create_statement: str) -> str:
    """Remove PRIMARY KEY, FOREIGN KEY and UNIQUE items from a CREATE TABLE column list. CHECK and NOT NULL stay."""
    open_at = create_statement.index("(")
    close_at = _matching_paren(create_statement, open_at)
    items = [item.strip() for item in _split_top_level(create_statement[open_at + 1:close_at])]
    kept = [item for item in items if item and not _INFORMATIONAL_CONSTRAINT.match(item)]
    return create_statement[:open_at + 1] + ",\n    ".join(kept) + create_statement[close_at:]


def run_ddl(spark: SparkSession, script: str, *, gold: str, databricks: bool | None = None) -> list[str]:
    """Run a DDL script. ${gold} is replaced with the gold schema name.

    Outside Databricks (the local Hive metastore) two things change:
    - PRIMARY KEY, FOREIGN KEY and UNIQUE are skipped, because Delta OSS does not support them. The skipped
      statements are returned so the caller can report them.
    - CREATE OR REPLACE TABLE becomes DROP TABLE + CREATE TABLE. Replacing a table with clustering and
      constraints makes Delta ask the Hive metastore to ALTER it, which fails here (HiveAlterHandler).
    """
    if databricks is None:
        databricks = is_databricks()
    skipped: list[str] = []
    for statement in split_statements(Template(script).substitute(gold=gold)):
        if databricks:
            spark.sql(statement)
            continue
        if is_informational_alter(statement):
            skipped.append(statement.splitlines()[0])
            continue
        if statement.upper().startswith("CREATE"):
            statement = strip_informational_constraints(statement)
        replaced = _CREATE_OR_REPLACE.match(statement)
        if replaced:
            spark.sql(f"DROP TABLE IF EXISTS {replaced.group(1)}")
            statement = re.sub(r"^(\s*)CREATE\s+OR\s+REPLACE\s+", r"\1CREATE ", statement, count=1, flags=re.IGNORECASE)
        spark.sql(statement)
    return skipped


def _matching_paren(text: str, open_at: int) -> int:
    depth = 0
    for index in range(open_at, len(text)):
        if text[index] == "(":
            depth += 1
        elif text[index] == ")":
            depth -= 1
            if depth == 0:
                return index
    raise ValueError("unbalanced parentheses in DDL statement")


def _split_top_level(text: str) -> list[str]:
    pieces, depth, start = [], 0, 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            pieces.append(text[start:index])
            start = index + 1
    pieces.append(text[start:])
    return pieces
