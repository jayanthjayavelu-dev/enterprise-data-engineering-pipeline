import logging

from src.common.spark_session import get_spark_session
from src.ingestion.users import load_users
from src.transformation.silver.users import clean_users
from src.loader.bronze_loader import write_bronze_users
from src.loader.silver_loader import write_silver_users
from src.common.logger import setup_logging


logger = logging.getLogger(__name__)


def main():
    setup_logging()

    spark = None

    try:
        spark = get_spark_session()

        bronze_user_df = load_users(spark)

        write_bronze_users(bronze_user_df)

        silver_user_df = clean_users(bronze_user_df)

        write_silver_users(silver_user_df)

        silver_user_df.printSchema()
        silver_user_df.show(5, truncate=False)


    except Exception:

        logger.exception("Users pipeline failed")
        raise

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
