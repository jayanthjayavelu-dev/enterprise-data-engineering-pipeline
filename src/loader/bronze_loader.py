import logging
from src.common.constant import (
    MINIO_BRONZE_USERS_PATH,
    MINIO_BRONZE_PRODUCTS_PATH,
    MINIO_BRONZE_CARTS_PATH
)

logger = logging.getLogger(__name__)


def write_bronze_users(raw_df):
    (raw_df
     .coalesce(1)
     .write.
     mode("overwrite")
     .parquet(MINIO_BRONZE_USERS_PATH)
     )

    logger.info("Successfully wrote Users DataFrame to Bronze layer")


def write_bronze_products(raw_df):
    (raw_df
     .coalesce(1)
     .write.
     mode("overwrite")
     .parquet(MINIO_BRONZE_PRODUCTS_PATH)
     )

    logger.info("Successfully wrote Product DataFrame to Bronze layer")


def write_bronze_carts(raw_df):
    (raw_df
     .coalesce(1)
     .write.
     mode("overwrite")
     .parquet(MINIO_BRONZE_CARTS_PATH)
     )

    logger.info("Successfully wrote Carts DataFrame to Bronze layer")
