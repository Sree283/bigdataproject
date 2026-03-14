from pyspark.sql.functions import *
from pyspark.sql.types import *


# ---------------------------------------------------
# 1. Parse Kafka JSON Events
# ---------------------------------------------------
def parse_events(df, event_schema):
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), event_schema).alias("data")) \
        .select("data.*")

    return parsed_df


# ---------------------------------------------------
# 2. Filter Bad Events
# ---------------------------------------------------
def filter_valid_events(df):

    valid_df = df.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("ad_id").isNotNull() &
        col("event_type").isin("view", "click")
    )

    return valid_df


# ---------------------------------------------------
# 3. Identify Bad Events (Quarantine)
# ---------------------------------------------------
def get_bad_events(df):

    bad_df = df.filter(
        col("event_id").isNull() |
        col("user_id").isNull() |
        col("ad_id").isNull()
    )

    return bad_df


# ---------------------------------------------------
# 4. Add Derived Columns
# ---------------------------------------------------
def enrich_events(df):

    enriched_df = df \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_hour", hour(col("event_time"))) \
        .withColumn("ingestion_time", current_timestamp())

    return enriched_df


# ---------------------------------------------------
# 5. Calculate Ad KPIs
# ---------------------------------------------------
def calculate_kpis(df):

    kpi_df = df.groupBy("ad_id", "event_date") \
        .agg(
            count(when(col("event_type") == "view", True)).alias("views"),
            count(when(col("event_type") == "click", True)).alias("clicks")
        ) \
        .withColumn(
            "ctr",
            when(col("views") > 0, col("clicks") / col("views")).otherwise(0)
        )

    return kpi_df


# ---------------------------------------------------
# 6. Write Clean Data to Hive
# ---------------------------------------------------
def write_to_hive(df):

    df.write \
        .mode("append") \
        .format("parquet") \
        .saveAsTable("ads_event_data")


# ---------------------------------------------------
# 7. Write KPI Metrics
# ---------------------------------------------------
def write_kpi_metrics(df):

    df.write \
        .mode("append") \
        .saveAsTable("ads_kpi_metrics")

