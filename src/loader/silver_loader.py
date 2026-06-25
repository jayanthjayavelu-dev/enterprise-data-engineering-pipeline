import logging

from src.common.constant import (
    MINIO_SILVER_USERS_PATH,
    MINIO_SILVER_PRODUCTS_PATH,
    MINIO_SILVER_CARTS_PATH,
)

logger = logging.getLogger(__name__)


def write_silver_users(cleansed_df):
    (
        cleansed_df.write.mode("overwrite")
        .partitionBy("processing_date")
        .parquet(MINIO_SILVER_USERS_PATH)
    )

    logger.info("Successfully wrote users DataFrame to Silver layer")


def write_silver_products(cleansed_df):
    (
        cleansed_df.write.mode("overwrite")
        .partitionBy("processing_date")
        .parquet(MINIO_SILVER_PRODUCTS_PATH)
    )
    logger.info("Successfully wrote Products DataFrame to Silver layer")


def write_silver_carts(cleansed_df):
    (
        cleansed_df.write.mode("overwrite")
        .partitionBy("processing_date")
        .parquet(MINIO_SILVER_CARTS_PATH)
    )
    logger.info("Successfully wrote Carts DataFrame to Silver layer")
