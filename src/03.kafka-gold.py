from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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


@dp.materialized_view(
    name="gold_latest_order_status",
    comment="Most recent status update per order (from order.updated)",
)
def gold_latest_order_status():
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())

    return (
        spark.read.table("silver_order_events")
        .filter(F.col("event_type") == "order.updated")
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "order_id",
            "new_status",
            "event_time",
            "topic",
            "partition",
            "offset",
        )
    )
