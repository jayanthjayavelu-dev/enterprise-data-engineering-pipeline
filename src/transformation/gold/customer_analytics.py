import logging

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from src.common.constant import (
    MINIO_SILVER_USERS_PATH,
    MINIO_SILVER_PRODUCTS_PATH,
    MINIO_SILVER_CARTS_PATH,
)

logger = logging.getLogger(__name__)


def build_customer_analytics(spark):
    users_df = spark.read.parquet(MINIO_SILVER_USERS_PATH)

    products_df = spark.read.parquet(MINIO_SILVER_PRODUCTS_PATH)

    carts_df = spark.read.parquet(MINIO_SILVER_CARTS_PATH)

    # Create Customer Analytics Dataset

    customer_analytics_df = (
        carts_df.alias("c")
        .join(users_df.alias("u"), F.col("c.carts_user_id") == F.col("u.id"), "left")
        .join(
            broadcast(products_df).alias("p"),
            F.col("c.cart_items_product_id") == F.col("p.product_id"),
            "left",
        )
        .withColumn(
            "discount_amount",
            (F.col("cart_items_total") - F.col("cart_items_discounted_total")).cast(
                "decimal(12,2)"
            ),
        )
        .withColumn("gross_revenue", F.col("cart_items_total"))
        .withColumn("net_revenue", F.col("cart_items_discounted_total"))
        .withColumn(
            "sales_to_stock_ratio",
            F.when(
                F.col("products_stock") > 0,
                F.round(F.col("cart_items_quantity") / F.col("products_stock"), 4),
            ).otherwise(0),
        )
        .withColumn(
            "customer_age_group",
            F.when(F.col("age") < 25, "Young")
            .when(F.col("age") < 40, "Adult")
            .when(F.col("age") < 60, "Middle Age")
            .otherwise("Senior"),
        )
    )

    # Customer Spend Metrics

    customer_spend_df = (
        customer_analytics_df.groupBy("carts_user_id")
        .agg(
            F.round(F.sum("net_revenue"), 2).alias("customer_total_spend"),
            F.sum("cart_items_quantity").alias("customer_total_quantity"),
            F.countDistinct("carts_id").alias("customer_total_orders"),
        )
        .withColumn(
            "customer_segment",
            F.when(F.col("customer_total_spend") >= 50000, "VIP")
            .when(F.col("customer_total_spend") >= 10000, "Premium")
            .otherwise("Regular"),
        )
    )

    # Merge Customer Metrics

    gold_df = (
        customer_analytics_df.alias("a")
        .join(
            customer_spend_df.alias("s"),
            F.col("a.carts_user_id") == F.col("s.carts_user_id"),
            "left",
        )
        .drop(
            users_df["processing_date"], products_df["processing_date"], carts_df["processing_date"]
        )
        .withColumn("processing_date", F.current_date())
        .select(
            F.col("a.carts_user_id").alias("customer_id"),
            F.col("firstName"),
            F.col("lastName"),
            F.col("age"),
            F.col("customer_age_group"),
            F.col("gender"),
            F.col("users_address_city"),
            F.col("users_address_state"),
            F.col("users_address_country"),
            F.col("product_id"),
            F.col("products_title"),
            F.col("products_category"),
            F.col("products_price"),
            F.col("products_rating"),
            F.col("products_stock"),
            F.col("carts_id"),
            F.col("cart_items_quantity"),
            F.col("gross_revenue"),
            F.col("net_revenue"),
            F.col("discount_amount"),
            F.col("sales_to_stock_ratio"),
            F.col("customer_total_spend"),
            F.col("customer_total_orders"),
            F.col("customer_total_quantity"),
            F.col("customer_segment"),
            F.col("processing_date"),
        )
    )

    gold_df.cache()
    # Validation

    record_count = gold_df.count()

    if record_count == 0:
        raise ValueError("Gold dataset contains no records")

    logger.info(f"Gold dataset record count: {record_count}")

    logger.info("Customer Analytics Dataframe - Created, Transformed and Validated")

    gold_df.unpersist()

    return gold_df
