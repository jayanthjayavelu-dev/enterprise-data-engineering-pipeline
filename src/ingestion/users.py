import requests
import json
import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StringType, StructField, LongType

logger = logging.getLogger(__name__)


def load_users(spark_session):
    logger.info("Reading WebAPI data")

    # Read API DATA
    data = read_api_data()

    logger.info("Creating User Dataframe")

    # Convert the Data into Dataframe with imposed schema using StructType
    df = build_dataframe(data, spark_session)

    logger.info("User Dataframe is created")

    return df


def build_dataframe(data_object, spark_session):
    rdd_data = spark_session.sparkContext.parallelize([json.dumps(data_object)])

    df = (spark_session.read.json(rdd_data)
          .withColumn("ingestion_timestamp",
                      current_timestamp()))

    return df


def read_api_data():
    try:
        response = requests.get("https://dummyjson.com/users",
                                timeout=(5, 30)
                                )
        response.raise_for_status()

    except requests.RequestException as exc:

        msg = f"API CALL FAILED: {exc}"

        logger.error(msg)

        raise RuntimeError(msg) from exc

    data = response.json()

    if not data:
        msg = "API returned empty dataset"
        logger.info(msg)
        raise RuntimeError(msg)

    return data
