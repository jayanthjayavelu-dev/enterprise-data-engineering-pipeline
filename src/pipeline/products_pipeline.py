import logging

from src.common.logger import setup_logging
from src.common.spark_session import get_spark_session
from src.ingestion.products import load_products
from src.loader.bronze_loader import write_bronze_products
from src.transformation.silver.products import clean_products
from src.loader.silver_loader import write_silver_products

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    spark = None

    try:
        spark = get_spark_session()

        bronze_user_df = load_products(spark)

        write_bronze_products(bronze_user_df)

        silver_products_df = clean_products(bronze_user_df)

        write_silver_products(silver_products_df)

    except Exception:

        logger.exception("Products pipeline failed")
        raise

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
