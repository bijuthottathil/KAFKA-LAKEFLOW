from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------
# GOLD LAYER — Order / payment analytics
# -----------------------------------------


@dp.materialized_view(
    name="gold_orders_by_region",
    comment="Order.created counts and revenue by region",
)
def gold_orders_by_region():
    return (
        spark.read.table("silver_order_events")
        .filter(F.col("event_type") == "order.created")
        .groupBy("region")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
    )


@dp.materialized_view(
    name="gold_payments_by_method",
    comment="Payment.authorized totals by payment method",
)
def gold_payments_by_method():
    return (
        spark.read.table("silver_order_events")
        .filter(F.col("event_type") == "payment.authorized")
        .groupBy("payment_method")
        .agg(
            F.count("*").alias("payment_count"),
            F.round(F.sum("payment_amount"), 2).alias("total_amount"),
        )
    )


@dp.materialized_view(
    name="gold_event_counts_by_type",
    comment="Row counts per event_type for pipeline monitoring",
)
def gold_event_counts_by_type():
    return (
        spark.read.table("silver_order_events")
        .groupBy("event_type")
        .agg(F.count("*").alias("event_count"))
    )


# -----------------------------------------
# SCD Type 2 — Order status history
#
# Tracks full history of order status changes.
# Each row gets __START_AT / __END_AT columns
# managed automatically by create_auto_cdc_flow.
# Active records have __END_AT = NULL.
#
# _seq (struct<event_time, offset>) is a composite
# sequence column: event_time is the primary sort,
# Kafka offset breaks ties when multiple events
# share the same millisecond timestamp.
# -----------------------------------------


@dp.view(name="v_order_status_updates")
def v_order_status_updates():
    """Intermediate streaming view: only order.updated events."""
    return (
        spark.readStream.table("silver_order_events")
        .filter(F.col("event_type") == "order.updated")
        .select(
            "order_id",
            "new_status",
            "event_time",
            "topic",
            "partition",
            "offset",
            F.struct(F.col("event_time"), F.col("offset")).alias("_seq"),
        )
    )


dp.create_streaming_table(
    name="gold_order_status_history",
    comment="SCD Type 2 history of order status changes (from order.updated)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    target="gold_order_status_history",
    source="v_order_status_updates",
    keys=["order_id"],
    sequence_by=F.col("_seq"),
    except_column_list=["_seq"],
    stored_as_scd_type="2",
)
