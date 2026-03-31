from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------
# BRONZE LAYER — Raw Kafka ingestion
#
# AppendFlow pattern: only new offsets are
# processed on each pipeline run (incremental).
# The table is protected from full resets so
# historical Kafka messages are never re-read.
# -----------------------------------------

dp.create_streaming_table(
    name="raw_kafka_orders",
    comment="Raw Kafka messages (orders.created, orders.updated, payments.authorized) — protected from full resets",
    table_properties={
        # Protect accumulated history from pipeline resets
        "pipelines.reset.allowed": "false",
        # Delta write optimisations
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        # Enable CDF for downstream CDC consumers
        "delta.enableChangeDataFeed": "true",
        # Retention safety
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.logRetentionDuration": "interval 30 days",
    },
)


@dp.append_flow(name="kafka_orders_ingest", target="raw_kafka_orders")
def kafka_orders_ingest():
    """
    Incremental Kafka source using append_flow.
    Databricks checkpoints the last committed offset per partition,
    so only new messages are read on each trigger — no duplicates,
    no full re-scans.
    """
    kafka_bootstrap = spark.conf.get("kafka.bootstrap.servers")
    kafka_api_key = spark.conf.get("kafka.api.key")
    kafka_api_secret = spark.conf.get("kafka.api.secret")
    kafka_topic = spark.conf.get("kafka.topic")

    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        f' required username="{kafka_api_key}" password="{kafka_api_secret}";'
    )

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("message_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
