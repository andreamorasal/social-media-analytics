from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

import os


# =========================================================
# CONFIGURATION
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "social_media_events"


# ---------------------------------------------------------
# Snowflake credentials
# ---------------------------------------------------------

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = "LAB_WH_AMS"

SNOWFLAKE_DATABASE = "SOCIAL_MEDIA_DB_AMS"
SNOWFLAKE_SCHEMA = "BRONZE"
SNOWFLAKE_TABLE = "RAW_EVENTS"


# =========================================================
# VALID EVENT TYPES
# =========================================================

VALID_EVENT_TYPES = [
    "post_created",
    "like",
    "comment",
    "share",
    "follow",
    "video_view",
    "profile_visit"
]


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("SocialMediaStreamingToSnowflake")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# EVENT SCHEMA
# =========================================================
#
# This MUST match the JSON produced by event_simulator.py
#
# =========================================================

event_schema = StructType([

    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),

    # USER INFORMATION
    StructField("user_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("country", StringType(), True),
    StructField("user_created_at", StringType(), True),

    # TARGET USER
    StructField("target_user_id", StringType(), True),
    StructField("target_username", StringType(), True),

    # POST INFORMATION
    StructField("post_id", StringType(), True),
    StructField("post_creator_user_id", StringType(), True),
    StructField("post_created_at", StringType(), True),
    StructField("hashtag", StringType(), True),

    # COMMENT
    StructField("comment_text", StringType(), True),

    # EVENT TIMESTAMP
    StructField("timestamp", StringType(), True)
])


# =========================================================
# READ FROM KAFKA
# =========================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# =========================================================
# CONVERT KAFKA VALUE TO JSON STRING
# =========================================================

json_stream = raw_stream.select(
    col("value").cast("string").alias("json_value")
)


# =========================================================
# PARSE JSON
# =========================================================

parsed_stream = json_stream.select(
    from_json(
        col("json_value"),
        event_schema
    ).alias("event")
)


# =========================================================
# EXTRACT EVENT FIELDS
# =========================================================

events = parsed_stream.select(

    col("event.event_id").alias("event_id"),
    col("event.event_type").alias("event_type"),

    # USER
    col("event.user_id").alias("user_id"),
    col("event.username").alias("username"),
    col("event.country").alias("country"),
    col("event.user_created_at").alias("user_created_at"),

    # TARGET USER
    col("event.target_user_id").alias("target_user_id"),
    col("event.target_username").alias("target_username"),

    # POST
    col("event.post_id").alias("post_id"),
    col("event.post_creator_user_id").alias(
        "post_creator_user_id"
    ),
    col("event.post_created_at").alias(
        "post_created_at"
    ),
    col("event.hashtag").alias("hashtag"),

    # COMMENT
    col("event.comment_text").alias("comment_text"),

    # EVENT TIMESTAMP
    col("event.timestamp").alias("event_timestamp")
)


# =========================================================
# DATA QUALITY CHECKS
# =========================================================

is_valid = (

    # Every event must have an ID
    col("event_id").isNotNull()

    # Event type must be one of the seven allowed types
    & col("event_type").isin(VALID_EVENT_TYPES)

    # Every event must have a user
    & col("user_id").isNotNull()

    # Every event must have an event timestamp
    & col("event_timestamp").isNotNull()

    # Events involving posts must have post information
    & (
        ~col("event_type").isin(
            [
                "post_created",
                "like",
                "comment",
                "share",
                "video_view"
            ]
        )
        |
        (
            col("post_id").isNotNull()
            & col("post_creator_user_id").isNotNull()
            & col("hashtag").isNotNull()
        )
    )

    # Comments must contain comment text
    & (
        (col("event_type") != "comment")
        |
        col("comment_text").isNotNull()
    )

    # Follows must have a target user
    & (
        (col("event_type") != "follow")
        |
        col("target_user_id").isNotNull()
    )

    # Profile visits must have a target user
    & (
        (col("event_type") != "profile_visit")
        |
        col("target_user_id").isNotNull()
    )
)


# =========================================================
# CLEAN EVENTS
# =========================================================

clean_events = (
    events
    .filter(is_valid)
    .dropDuplicates(["event_id"])
    .select(
        "event_id",
        "event_type",

        "user_id",
        "username",
        "country",
        "user_created_at",

        "target_user_id",
        "target_username",

        "post_id",
        "post_creator_user_id",
        "post_created_at",
        "hashtag",

        "comment_text",

        "event_timestamp"
    )
)


# =========================================================
# SNOWFLAKE CONNECTION
# =========================================================

def get_snowflake_connection():

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        paramstyle="qmark"
    )


# =========================================================
# WRITE MICRO-BATCH TO SNOWFLAKE
# =========================================================

def write_to_snowflake(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    connection = None
    cursor = None

    try:

        connection = get_snowflake_connection()
        cursor = connection.cursor()

        # -------------------------------------------------
        # Insert all expanded event fields.
        #
        # INGESTED_AT is intentionally NOT included because
        # Snowflake generates it using DEFAULT CURRENT_TIMESTAMP().
        # -------------------------------------------------

        insert_sql = f"""
            INSERT INTO {SNOWFLAKE_TABLE}
            (
                EVENT_ID,
                EVENT_TYPE,

                USER_ID,
                USERNAME,
                COUNTRY,
                USER_CREATED_AT,

                TARGET_USER_ID,
                TARGET_USERNAME,

                POST_ID,
                POST_CREATOR_USER_ID,
                POST_CREATED_AT,
                HASHTAG,

                COMMENT_TEXT,

                EVENT_TIMESTAMP
            )
            VALUES (
                ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?,
                ?
            )
        """

        data = []

        for row in rows:

            data.append(
                (
                    row["event_id"],
                    row["event_type"],

                    row["user_id"],
                    row["username"],
                    row["country"],
                    row["user_created_at"],

                    row["target_user_id"],
                    row["target_username"],

                    row["post_id"],
                    row["post_creator_user_id"],
                    row["post_created_at"],
                    row["hashtag"],

                    row["comment_text"],

                    row["event_timestamp"]
                )
            )

        cursor.executemany(
            insert_sql,
            data
        )

        connection.commit()

        print(
            f"Batch {batch_id} written to Snowflake: "
            f"{len(data)} records"
        )

    except Exception as e:

        if connection is not None:
            connection.rollback()

        print(
            f"ERROR writing batch {batch_id} "
            f"to Snowflake:"
        )

        print(e)

    finally:

        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


# =========================================================
# START STREAMING QUERY
# =========================================================

snowflake_query = (
    clean_events
    .writeStream
    .foreachBatch(write_to_snowflake)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "./checkpoints/snowflake_raw_v3"
    )
    .start()
)


# =========================================================
# KEEP STREAMING
# =========================================================

spark.streams.awaitAnyTermination()
