"""Fact and bridge builders. Each one has a source step (join and change timestamp) and a shape step
(surrogate keys and the target columns). The notebook filters the source to the increment in between."""
from pyspark.sql import DataFrame, functions as F

from gold.incremental import CHANGE_TS, with_change_timestamps
from gold.keys import date_key, surrogate_key
from gold.lookup import key_lookup, point_in_time_key


def _lineage(df: DataFrame, run_id: str) -> DataFrame:
    """Pipeline lineage. _source_batch_id is set by the caller from the silver row."""
    return df.withColumns({
        "_pipeline_run_id": F.lit(run_id),
        "_gold_loaded_timestamp": F.current_timestamp(),
    })


# ---------------------------------------------------------------- fact_sales

def fact_sales_source(order_items: DataFrame, orders: DataFrame) -> DataFrame:
    """One row per order item with its order's context. Change time is the newer of the two."""
    order_context = with_change_timestamps(orders).select(
        "order_id", "customer_id", "billing_address_id", "shipping_address_id", "order_timestamp",
        "channel", "order_status", "currency_code",
        F.col(CHANGE_TS).alias("_order_change_ts"),
    )
    items = with_change_timestamps(order_items)
    return (
        items.join(order_context, on="order_id", how="left")
        .withColumn(CHANGE_TS, F.greatest(F.col(CHANGE_TS), F.col("_order_change_ts")))
        .drop("_order_change_ts")
    )


def shape_fact_sales(
    source: DataFrame, customers: DataFrame, addresses: DataFrame,
    products: DataFrame, dates: DataFrame, run_id: str,
) -> DataFrame:
    keyed = point_in_time_key(source, customers, fact_key="customer_id", dim_key="customer_id",
                              fact_time="order_timestamp", dim_sk="customer_sk", output="customer_sk")
    keyed = point_in_time_key(keyed, addresses, fact_key="billing_address_id", dim_key="address_id",
                              fact_time="order_timestamp", dim_sk="address_sk", output="billing_address_sk")
    keyed = point_in_time_key(keyed, addresses, fact_key="shipping_address_id", dim_key="address_id",
                              fact_time="order_timestamp", dim_sk="address_sk", output="shipping_address_sk")
    keyed = point_in_time_key(keyed, products, fact_key="product_id", dim_key="product_id",
                              fact_time="order_timestamp", dim_sk="product_sk", output="product_sk")
    keyed = key_lookup(keyed, dates, fact_key=date_key("order_timestamp"), dim_key="date_key",
                       dim_sk="date_sk", output="order_date_sk")
    shaped = keyed.select(
        surrogate_key("order_item_id").alias("sales_sk"),
        "order_id", "order_item_id",
        "customer_sk", "billing_address_sk", "shipping_address_sk", "product_sk", "order_date_sk",
        "channel", "order_status", "currency_code",
        F.col("quantity").cast("int"),
        F.col("unit_price").cast("decimal(18,2)"),
        F.col("discount_amount").cast("decimal(18,2)"),
        F.col("tax_amount").cast("decimal(18,2)"),
        F.col("line_total_amount").cast("decimal(18,2)"),
        "_source_batch_id",
    )
    return _lineage(shaped, run_id)


# ---------------------------------------------------------------- fact_payment

def fact_payment_source(payments: DataFrame) -> DataFrame:
    return with_change_timestamps(payments)


def shape_fact_payment(source: DataFrame, customers: DataFrame, dates: DataFrame, run_id: str) -> DataFrame:
    keyed = point_in_time_key(source, customers, fact_key="customer_id", dim_key="customer_id",
                              fact_time="payment_timestamp", dim_sk="customer_sk", output="customer_sk")
    keyed = key_lookup(keyed, dates, fact_key=date_key("payment_timestamp"), dim_key="date_key",
                       dim_sk="date_sk", output="payment_date_sk")
    shaped = keyed.select(
        surrogate_key("payment_id").alias("payment_sk"),
        "payment_id", "order_id", "customer_sk", "payment_date_sk",
        "payment_method", "payment_status", "currency_code", "transaction_reference",
        F.col("amount").cast("decimal(18,2)").alias("payment_amount"),
        "_source_batch_id",
    )
    return _lineage(shaped, run_id)


# ---------------------------------------------------------------- fact_return

def fact_return_source(return_items: DataFrame, returns: DataFrame) -> DataFrame:
    return_context = with_change_timestamps(returns).select(
        "return_id", "customer_id", "return_timestamp", "return_status",
        F.col(CHANGE_TS).alias("_return_change_ts"),
    )
    items = with_change_timestamps(return_items)
    return (
        items.join(return_context, on="return_id", how="left")
        .withColumn(CHANGE_TS, F.greatest(F.col(CHANGE_TS), F.col("_return_change_ts")))
        .drop("_return_change_ts")
    )


def shape_fact_return(
    source: DataFrame, customers: DataFrame, products: DataFrame, dates: DataFrame, run_id: str,
) -> DataFrame:
    keyed = point_in_time_key(source, customers, fact_key="customer_id", dim_key="customer_id",
                              fact_time="return_timestamp", dim_sk="customer_sk", output="customer_sk")
    keyed = point_in_time_key(keyed, products, fact_key="product_id", dim_key="product_id",
                              fact_time="return_timestamp", dim_sk="product_sk", output="product_sk")
    keyed = key_lookup(keyed, dates, fact_key=date_key("return_timestamp"), dim_key="date_key",
                       dim_sk="date_sk", output="return_date_sk")
    shaped = keyed.select(
        surrogate_key("return_item_id").alias("return_sk"),
        "return_id", "return_item_id", "order_item_id",
        "customer_sk", "product_sk", "return_date_sk",
        "return_reason", "return_status",
        F.col("quantity").cast("int"),
        F.col("refund_amount").cast("decimal(18,2)"),
        "_source_batch_id",
    )
    return _lineage(shaped, run_id)


# ---------------------------------------------------------------- bridge_product_category

def bridge_source(product_categories: DataFrame) -> DataFrame:
    """The product-category link. Its time is when the link became active (active_from, else created_at)."""
    rows = with_change_timestamps(product_categories)
    return rows.withColumn("_link_time", F.coalesce(F.col("active_from").cast("timestamp"), F.col("created_at")))


def shape_bridge(source: DataFrame, products: DataFrame, categories: DataFrame, run_id: str) -> DataFrame:
    keyed = point_in_time_key(source, products, fact_key="product_id", dim_key="product_id",
                              fact_time="_link_time", dim_sk="product_sk", output="product_sk")
    keyed = key_lookup(keyed, categories, fact_key="category_id", dim_key="category_id",
                       dim_sk="category_sk", output="category_sk")
    shaped = keyed.select(
        surrogate_key("product_category_id").alias("bridge_sk"),
        "product_category_id", "product_sk", "category_sk",
        "is_primary", "active_from", "active_to",
        "_source_batch_id",
    )
    return _lineage(shaped, run_id)
