from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ----------------------------------------
# Schema for incoming Kafka JSON events
# ----------------------------------------

ads_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("country", StringType(), True)
])


# ----------------------------------------
# Parse Kafka JSON value
# ----------------------------------------

def parse_kafka_events(df):

    parsed_df = df.select(
        from_json(col("value").cast("string"), ads_event_schema).alias("data")
    ).select("data.*")

    return parsed_df


# ----------------------------------------
# Data Cleaning
# ----------------------------------------

def clean_events(df):

    cleaned_df = df.filter(
        col("event_id").isNotNull() &
        col("event_type").isNotNull() &
        col("ad_id").isNotNull()
    )

    return cleaned_df


# ----------------------------------------
# Convert timestamp format
# ----------------------------------------

def convert_timestamp(df):

    df = df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    )

    return df


# ----------------------------------------
# Add derived columns
# ----------------------------------------

def enrich_events(df):

    df = df.withColumn(
        "is_click",
        when(col("event_type") == "click", 1).otherwise(0)
    ).withColumn(
        "is_impression",
        when(col("event_type") == "impression", 1).otherwise(0)
    )

    return df


# ----------------------------------------
# KPI Aggregations
# ----------------------------------------

def calculate_kpis(df):

    kpi_df = df.groupBy("campaign_id").agg(
        {"is_click": "sum", "is_impression": "sum"}
    ).withColumnRenamed(
        "sum(is_click)", "total_clicks"
    ).withColumnRenamed(
        "sum(is_impression)", "total_impressions"
    )

    return kpi_df
