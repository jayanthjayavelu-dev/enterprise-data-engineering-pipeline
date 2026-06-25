import pytest
from pyspark.sql import SparkSession

from src.transformation.silver.carts import clean_carts


@pytest.fixture(scope="session")
def spark_obj():
    spark = SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()

    yield spark

    spark.stop()


def test_completed_task(spark_obj):
    spark = spark_obj

    from decimal import Decimal

    data = [
        (
            1,  # carts_id
            15,  # carts_user_id
            Decimal("5499.00"),  # carts_total
            Decimal("4999.00"),  # carts_discounted_total
            2,  # carts_total_products
            3,  # carts_total_quantity
            101,  # cart_items_id
            1,  # cart_items_cart_id
            5,  # cart_items_product_id
            "iPhone Charger",  # cart_items_title
            Decimal("999.00"),  # cart_items_price
            2,  # cart_items_quantity
            Decimal("1998.00"),  # cart_items_total
            Decimal("10.00"),  # cart_items_discount_percentage
            Decimal("1798.20"),  # cart_items_discounted_total
            "https://example.com/charger.jpg",  # cart_items_thumbnail
        )
    ]

    columns = [
        "carts_id",
        "carts_user_id",
        "carts_total",
        "carts_discounted_total",
        "carts_total_products",
        "carts_total_quantity",
        "cart_items_id",
        "cart_items_cart_id",
        "cart_items_product_id",
        "cart_items_title",
        "cart_items_price",
        "cart_items_quantity",
        "cart_items_total",
        "cart_items_discount_percentage",
        "cart_items_discounted_total",
        "cart_items_thumbnail",
    ]

    df = spark.createDataFrame(data, columns)

    result_df = clean_carts(df)

    assert result_df.count() == 1
