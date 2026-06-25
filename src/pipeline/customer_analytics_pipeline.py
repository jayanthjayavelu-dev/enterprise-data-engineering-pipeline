import logging

from src.loader.gold_loader import load_gold_to_minio, load_gold_to_postgres
from src.common.spark_session import get_spark_session
from src.common.logger import setup_logging
from src.transformation.gold.customer_analytics import build_customer_analytics

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    spark = None

    try:
        spark = get_spark_session()

        gold_customer_analytics_df = build_customer_analytics(spark)

        load_gold_to_postgres(gold_customer_analytics_df)

        load_gold_to_minio(gold_customer_analytics_df)

    except Exception:
        logger.exception("Users pipeline failed")
        raise

    finally:
        logger.info("Successfully Completed Customer Analytics Data pipeline")

        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
