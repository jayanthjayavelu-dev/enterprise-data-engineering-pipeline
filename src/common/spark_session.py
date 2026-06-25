from pyspark.sql import SparkSession
from config.spark_dependencies import SPARK_PACKAGES
from src.common.constant import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MONGO_READ_CONNECTION_URI,
    SPARK_MASTER,
)


def get_spark_session():
    spark = (
        SparkSession.builder.appName("MinIOTest")
        .master(SPARK_MASTER)
        # AWS CONFIG
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # SPARK PACKAGES
        .config("spark.jars.packages", SPARK_PACKAGES)
        # MONGO READ CONFIG
        .config("spark.mongodb.read.connection.uri", MONGO_READ_CONNECTION_URI)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
