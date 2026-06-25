import pytest
from pyspark.sql import SparkSession

from transformation import clean_todos


@pytest.fixture(scope="session")
def spark_obj():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_completed_task(spark_obj):
    spark = spark_obj

    data = [
        (1, 1, "delectus aut autem", False),
        (1, 2, "quis ut nam facilis et officia qui", False),
        (1, 3, "fugiat veniam minus", False),
        (1, 4, "et porro tempora", True),
        (1, 5, "laboriosam mollitia et enim quasi adipisci quia provident illum", False),
    ]

    columns = ["userId", "id", "title", "completed"]

    df = spark.createDataFrame(data, columns)

    result_df = clean_todos(df)

    assert result_df.count() == 1
