import logging

from pyspark.sql.functions import current_timestamp

logger = logging.getLogger(__name__)


def load_products(spark_session):
    logger.info("Reading MongoDB data from Product Collection")

    df = (
        spark_session.read.format("mongodb")
        .load()
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    logger.info("Product Dataframe is created")

    return df
