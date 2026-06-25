import logging

from pyspark.sql.functions import col, trim, explode, current_timestamp

logger = logging.getLogger(__name__)


def clean_users(df):
    explode_df = df.withColumn("users", explode("users"))

    flatten_df = explode_df.selectExpr(
        "users.id",
        "users.firstName",
        "users.lastName",
        "users.maidenName",
        "users.age",
        "users.gender",
        "users.email",
        "users.phone",
        "users.username",
        "users.password",
        "users.birthDate",
        "users.image",
        "users.bloodGroup",
        "users.height",
        "users.weight",
        "users.eyeColor",
        "users.hair.color AS users_hair_color",
        "users.hair.type AS users_hair_type",
        "users.ip",
        "users.address.address AS users_address_address",
        "users.address.city AS users_address_city",
        "users.address.coordinates.lat AS users_address_coordinates_lat",
        "users.address.coordinates.lng AS users_address_coordinates_lng",
        "users.address.country AS users_address_country",
        "users.address.postalCode AS users_address_postalCode",
        "users.address.state AS users_address_state",
        "users.address.stateCode AS users_address_stateCode",
        "users.macAddress AS users_macAddress",
        "users.university AS users_university",
        "users.bank.cardExpire AS users_bank_cardExpire",
        "users.bank.cardNumber AS users_bank_cardNumber",
        "users.bank.cardType AS users_bank_cardType",
        "users.bank.currency AS users_bank_currency",
        "users.bank.iban AS users_bank_iban",
        "users.company.department AS users_company_department",
        "users.company.name AS users_company_name",
        "users.company.title AS users_company_title",
        "users.company.address.address AS users_company_address_address",
        "users.company.address.city AS users_company_address_city",
        "users.company.address.state AS users_company_address_state",
        "users.company.address.stateCode AS users_company_address_stateCode",
        "users.company.address.postalCode AS users_company_address_postalCode",
        "users.company.address.coordinates.lat AS users_company_address_coordinates_lat",
        "users.company.address.coordinates.lng AS users_company_address_coordinates_lng",
        "users.company.address.country AS users_company_address_country",
        "users.ein AS users_ein",
        "users.ssn AS users_ssn",
        "users.userAgent AS users_userAgent",
        "users.crypto.coin AS users_crypto_coin",
        "users.crypto.wallet AS users_crypto_wallet",
        "users.crypto.network AS users_crypto_network",
        "users.role AS users_role",
        "total",
        "skip",
        "limit",
    ).withColumn("processed_timestamp", current_timestamp())

    cleansed_df = (
        flatten_df.withColumn("firstName", trim("firstName"))
        .withColumn("lastName", trim("lastName"))
        .withColumn("maidenName", trim("maidenName"))
    )

    data_quality_check(cleansed_df)

    logger.info("User Dataframe is Cleansed and Validated")

    return cleansed_df


def data_quality_check(df):
    duplicate_count = df.groupBy("id").count().filter("count > 1").count()

    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count} duplicate IDs")
        raise ValueError(f"Found {duplicate_count} duplicate IDs")

    logger.info(
        f"Duplicate ID count: {duplicate_count}",
    )

    required_columns = ["id", "firstName", "email"]

    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()

        if null_count > 0:
            logger.error(f"Found {null_count} Null values in {col_name} Column")
            raise ValueError(f"Found {null_count} Null values in {col_name} Column")

        logger.info(f"Null count in {col_name}: {null_count}")
