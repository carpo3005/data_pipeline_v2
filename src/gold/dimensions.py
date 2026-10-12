"""Dimension definitions and the mapping from silver rows to dimension columns."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gold.incremental import CHANGE_TS, FIRST_CHANGE_TS
from gold.keys import attribute_hash, surrogate_key
from gold.scd.scd2 import Scd2Table

CUSTOMER = Scd2Table(
    name="dim_customer",
    business_key="customer_id",
    surrogate_key="customer_sk",
    attributes=("first_name", "last_name", "email_hash", "email_domain", "phone", "date_of_birth",
                "loyalty_status", "marketing_opt_in"),
)
ADDRESS = Scd2Table(
    name="dim_address",
    business_key="address_id",
    surrogate_key="address_sk",
    attributes=("customer_id", "address_type", "address_line_1", "address_line_2", "city", "state_region",
                "country_subdivision_code", "postal_code", "country_code", "is_primary"),
)
PRODUCT = Scd2Table(
    name="dim_product",
    business_key="product_id",
    surrogate_key="product_sk",
    attributes=("sku", "product_name", "brand", "unit_price", "currency_code", "is_active"),
)
CATEGORY_ATTRIBUTES = ("category_name", "parent_category_id", "category_code", "is_active")  # SCD1 dim_category


def _normalised_email(email_column: str) -> Column:
    return F.lower(F.trim(F.col(email_column)))


def email_hash(email_column: str, salt: str) -> Column:
    """Salted SHA-256 of the trimmed, lower-cased email. Blank or NULL emails give NULL.

    The same address always gives the same hash, so a customer can still be matched across loads, but the
    address itself is not stored in Gold. Changing the salt changes every hash, so it must stay fixed.
    """
    normalised = _normalised_email(email_column)
    return F.when(normalised != "", F.sha2(F.concat(F.lit(salt), normalised), 256))


def shape_customer(customers: DataFrame, email_salt: str) -> DataFrame:
    """Gold customer rows. The raw email is replaced by email_hash; only its domain is kept in clear."""
    if not email_salt:
        raise ValueError("shape_customer needs an email_salt: customer emails are never stored unhashed in Gold")
    normalised = _normalised_email("email")
    email_domain = F.when(normalised.contains("@"), F.substring_index(normalised, "@", -1))
    return customers.select(
        "customer_id", "first_name", "last_name",
        email_hash("email", email_salt).alias("email_hash"),
        email_domain.alias("email_domain"),
        "phone", "date_of_birth", "loyalty_status", "marketing_opt_in",
        "_source_batch_id", CHANGE_TS, FIRST_CHANGE_TS,
    )


def shape_address(addresses: DataFrame) -> DataFrame:
    return addresses.select(
        "address_id", "customer_id", "address_type", "address_line_1", "address_line_2", "city", "state_region",
        # No silver source yet: silver has uk_nation, which is a nation, not a subdivision code. Left NULL.
        F.lit(None).cast("string").alias("country_subdivision_code"),
        "postal_code", "country_code", "is_primary",
        "_source_batch_id", CHANGE_TS, FIRST_CHANGE_TS,
    )


def shape_product(products: DataFrame) -> DataFrame:
    return products.select(
        "product_id", "sku", "product_name", "brand",
        F.col("unit_price").cast("decimal(18,4)").alias("unit_price"),
        "currency_code", "is_active",
        "_source_batch_id", CHANGE_TS, FIRST_CHANGE_TS,
    )


def shape_category(categories: DataFrame, run_id: str) -> DataFrame:
    """SCD1 category: one row per category_id, keyed by a stable surrogate key."""
    return categories.select(
        surrogate_key("category_id").alias("category_sk"),
        "category_id",
        *CATEGORY_ATTRIBUTES,
        attribute_hash(CATEGORY_ATTRIBUTES).alias("attribute_hash"),
        "_source_batch_id",
        F.lit(run_id).alias("_pipeline_run_id"),
        F.current_timestamp().alias("_gold_loaded_timestamp"),
    )
