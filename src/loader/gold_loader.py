import logging

from src.common.constant import (
    MINIO_GOLD_PATH,
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DRIVER,
)

logger = logging.getLogger(__name__)


def load_gold_to_minio(df):
    (df.write.mode("overwrite").parquet(MINIO_GOLD_PATH))

    logger.info("Customer Analytics data wrote to minIO")


def load_gold_to_postgres(df):
    (
        df.write.mode("overwrite")
        .format("jdbc")
        .options(
            url=POSTGRES_URL,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbtable="customer_analytics",
            driver=POSTGRES_DRIVER,
        )
        .save()
    )

    logger.info("Customer Analytics data Inserted to Postgres Table")
