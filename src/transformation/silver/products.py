import logging

from pyspark.sql.functions import col, explode, current_date

logger = logging.getLogger(__name__)


def clean_products(df):
    exploded_df = df.withColumn("products", explode("products"))

    flattened_df = exploded_df.selectExpr(
        "products.id AS product_id",
        "products.title AS products_title",
        "products.description AS products_description",
        "products.category AS products_category",
        "products.price AS products_price",
        "products.discountPercentage AS products_discountPercentage",
        "products.rating AS products_rating",
        "products.stock AS products_stock",
        "products.tags AS products_tags",
        "products.sku AS products_sku",
        "products.weight AS products_weight",
        "products.dimensions.width AS products_dimensions_width",
        "products.dimensions.height AS products_dimensions_height",
        "products.dimensions.depth AS products_dimensions_depth",
        "products.warrantyInformation AS products_warrantyInformation",
        "products.shippingInformation AS products_shippingInformation",
        "products.availabilityStatus AS products_availabilityStatus",
        "products.reviews AS products_reviews",
        "products.returnPolicy AS products_returnPolicy",
        "products.minimumOrderQuantity AS products_minimumOrderQuantity",
        "products.meta.createdAt AS products_meta_createdAt",
        "products.meta.updatedAt AS products_meta_updatedAt",
        "products.meta.barcode AS products_meta_barcode",
        "products.meta.qrCode AS products_meta_qrCode",
        "products.images AS products_images",
        "products.thumbnail AS products_thumbnail",
        "total",
        "skip",
        "limit",
    ).withColumn("processing_date", current_date())

    data_quality_check(flattened_df)

    logger.info("Products DataFrame cleansed and validated")

    return flattened_df


def data_quality_check(df):
    duplicate_count = df.groupBy("product_id").count().filter("count > 1").count()

    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count} duplicate product IDs")
        raise ValueError(f"Found {duplicate_count} duplicate product IDs")

    logger.info(
        f"Duplicate ID count: {duplicate_count}",
    )

    required_columns = ["product_id", "products_title", "products_price"]

    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()

        if null_count > 0:
            logger.error(f"Found {null_count} Null values in {col_name} Column")
            raise ValueError(f"Found {null_count} Null values in {col_name} Column")

        logger.info(f"Null count in {col_name}: {null_count}")
