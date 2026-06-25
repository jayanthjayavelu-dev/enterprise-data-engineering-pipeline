import logging

from src.ingestion.carts import load_carts
from src.loader.bronze_loader import write_bronze_carts
from src.loader.silver_loader import write_silver_carts
from src.common.logger import setup_logging
from src.common.spark_session import get_spark_session
from src.common.constant import POSTGRES_RAW_TABLES
from src.transformation.silver.carts import clean_carts

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    spark = None

    try:
        spark = get_spark_session()

        bronze_carts_df = load_carts(spark, POSTGRES_RAW_TABLES)

        write_bronze_carts(bronze_carts_df)

        silver_carts_df = clean_carts(bronze_carts_df)

        write_silver_carts(silver_carts_df)

    except Exception:
        logger.exception("Products pipeline failed")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
