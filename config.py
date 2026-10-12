# src/pipeline/config.py
import os
from pathlib import Path

from pipeline.session import is_databricks

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

EMAIL_MASK_SALT_VAR = "EMAIL_MASK_SALT"

def get_email_mask_salt() -> str:
    """Salt for the gold email hash. Set it in the environment so it is never committed. Fails if unset."""
    salt = os.environ.get(EMAIL_MASK_SALT_VAR, "")
    if not salt:
        raise RuntimeError(f"Environment variable {EMAIL_MASK_SALT_VAR} is not set; gold customer load cannot hash emails")
    return salt
