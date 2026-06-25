from pyspark.sql.functions import col, current_timestamp

from src.common.constant import POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER


def load_carts(spark_session, tables):
    df = {}

    for table in tables:
        df[table] = (
            spark_session.read.format("jdbc")
            .options(
                url=POSTGRES_URL,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbtable=table,
                driver=POSTGRES_DRIVER,
            )
            .load()
        )
    join_carts_df = (
        df["carts"]
        .alias("c")
        .join(df["cart_items"].alias("i"), col("c.id") == col("i.cart_id"), "inner")
        .selectExpr(
            "c.id AS carts_id",
            "c.user_id AS carts_user_id",
            "c.total AS carts_total",
            "c.discounted_total AS carts_discounted_total",
            "c.total_products AS carts_total_products",
            "c.total_quantity AS carts_total_quantity",
            "i.id AS cart_items_id",
            "i.cart_id AS cart_items_cart_id",
            "i.product_id AS cart_items_product_id",
            "i.title AS cart_items_title",
            "i.price AS cart_items_price",
            "i.quantity AS cart_items_quantity",
            "i.total AS cart_items_total",
            "i.discount_percentage AS cart_items_discount_percentage",
            "i.discounted_total AS cart_items_discounted_total",
            "i.thumbnail AS cart_items_thumbnail",
        )
    ).withColumn("processed_timestamp", current_timestamp())

    return join_carts_df
