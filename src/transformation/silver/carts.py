import logging

from pyspark.sql.functions import col, explode, current_timestamp

logger = logging.getLogger(__name__)


def clean_carts(df):
    data_quality_check(df)

    logger.info("Carts DataFrame validated")

    return df


def data_quality_check(df):
    duplicate_count = (df
                       .groupBy("cart_items_id")
                       .count()
                       .filter("count > 1")
                       .count()
                       )

    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count} duplicate Cart item IDs")
        raise ValueError(f"Found {duplicate_count} duplicate Cart item IDs")

    logger.info(f"Duplicate ID count: {duplicate_count}", )

    required_columns = ["user_id", "cart_id", "product_id", "title"]

    for col_name in required_columns:

        null_count = df.filter(col(col_name).isNull()).count()

        if null_count > 0:
            logger.error(f"Found {null_count} Null values in {col_name} Column")
            raise ValueError(f"Found {null_count} Null values in {col_name} Column")

        logger.info(f"Null count in {col_name}: {null_count}")
