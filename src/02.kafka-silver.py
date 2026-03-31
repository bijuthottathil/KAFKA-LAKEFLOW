from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# -----------------------------------------
# SILVER LAYER — Parse + cleanse simulator payloads
#
# Event shapes (JSON) from the notebook simulator:
# - order.created:   event_type, event_time, order_id, customer_id, region, items[], total_amount
# - order.updated:   event_type, event_time, order_id, new_status
# - payment.authorized: event_type, event_time, payment_id, order_id, amount, method
# -----------------------------------------

ITEM_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
    ]
)

ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("items", ArrayType(ITEM_SCHEMA), True),
        StructField("total_amount", DoubleType(), True),
        StructField("new_status", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("method", StringType(), True),
    ]
)


@dp.table(
    name="silver_order_events",
    comment="Parsed order / payment events from Kafka (simulator JSON)",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
@dp.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
def silver_order_events():
    return (
        spark.readStream.table("raw_kafka_orders")
        .withColumn("payload", F.from_json(F.col("message_value"), ORDER_EVENT_SCHEMA))
        .select(
            F.col("payload.event_type").alias("event_type"),
            F.to_timestamp(F.col("payload.event_time")).alias("event_time"),
            F.col("payload.order_id").alias("order_id"),
            F.col("payload.customer_id").alias("customer_id"),
            F.col("payload.region").alias("region"),
            F.col("payload.items").alias("items"),
            F.col("payload.total_amount").alias("total_amount"),
            F.col("payload.new_status").alias("new_status"),
            F.col("payload.payment_id").alias("payment_id"),
            F.col("payload.amount").alias("payment_amount"),
            F.col("payload.method").alias("payment_method"),
            F.col("kafka_timestamp"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("ingested_at"),
        )
    )
