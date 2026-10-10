# src/pipeline/config.py
from pipeline.session import is_databricks

from pathlib import Path

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
CATALOGUE = "forge"
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if is_databricks():
    INPUT_ROOT = f"/Volumes/{CATALOGUE}/{BRONZE_SCHEMA}/new_src/"     # or abfss:// / s3:// paths
else:
    INPUT_ROOT = str(PROJECT_ROOT / "data")

# This function belongs somewhere else probably but I haven't decided where yet
def get_csv_path(batch_id: str, entity: str) -> str:
    return f"{INPUT_ROOT}/{batch_id}/{entity}_{batch_id}.csv"
