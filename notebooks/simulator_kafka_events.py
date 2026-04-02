# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka event simulator (orders / payments)
# MAGIC Run after setting cluster Spark conf or edit `NUM_EVENTS` below.
# MAGIC Uses the same Confluent settings as the Lakeflow pipeline (`kafka.*`).

# COMMAND ----------

# DBTITLE 1,Cell 2
import json
import random
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row

# Match pipeline `simulation.num_events` when run on a cluster with that conf
try:
    NUM_EVENTS = int(spark.conf.get("simulation.num_events"))
except Exception:
    NUM_EVENTS = 50

CUSTOMERS = [f"CUST-{i:04d}" for i in range(1, 20)]
PRODUCTS = [
    {"id": "PROD-001", "name": "Laptop Pro", "price": 1299.99},
    {"id": "PROD-002", "name": "Wireless Mouse", "price": 49.99},
    {"id": "PROD-003", "name": "USB-C Hub", "price": 79.99},
    {"id": "PROD-004", "name": "Monitor 27in", "price": 399.99},
    {"id": "PROD-005", "name": "Keyboard", "price": 149.99},
]
REGIONS = ["US-East", "US-West", "EU-West", "APAC"]
STATUSES = ["pending", "confirmed", "shipped", "delivered"]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


print("Sample data loaded")
print(f"Generating {NUM_EVENTS} events...")

orders_created = []
orders_updated = []
payments = []
existing_orders = []

for i in range(NUM_EVENTS):
    event_type = random.choices(["create", "update", "payment"], weights=[5, 3, 2])[0]

    if event_type == "create" or not existing_orders:
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        items = random.sample(PRODUCTS, k=random.randint(1, 3))
        total = sum(p["price"] * random.randint(1, 2) for p in items)

        event = {
            "event_type": "order.created",
            "event_time": now_iso(),
            "order_id": order_id,
            "customer_id": random.choice(CUSTOMERS),
            "region": random.choice(REGIONS),
            "items": [{"product_id": p["id"], "name": p["name"], "price": p["price"]} for p in items],
            "total_amount": round(total, 2),
        }
        orders_created.append(Row(key=order_id, value=json.dumps(event)))
        existing_orders.append({"id": order_id, "amount": total})

    elif event_type == "update":
        order = random.choice(existing_orders)
        event = {
            "event_type": "order.updated",
            "event_time": now_iso(),
            "order_id": order["id"],
            "new_status": random.choice(STATUSES),
        }
        orders_updated.append(Row(key=order["id"], value=json.dumps(event)))

    else:
        order = random.choice(existing_orders)
        event = {
            "event_type": "payment.authorized",
            "event_time": now_iso(),
            "payment_id": f"PAY-{uuid.uuid4().hex[:8].upper()}",
            "order_id": order["id"],
            "amount": order["amount"],
            "method": random.choice(PAYMENT_METHODS),
        }
        payments.append(Row(key=order["id"], value=json.dumps(event)))

print(
    f"Generated: {len(orders_created)} orders, {len(orders_updated)} updates, {len(payments)} payments"
)

# COMMAND ----------

# DBTITLE 1,Cell 3
try:
    kafka_bootstrap = spark.conf.get("kafka.bootstrap.servers")
except Exception:
    kafka_bootstrap = "yourserver.confluent.cloud:9092"

try:
    kafka_api_key = spark.conf.get("kafka.api.key")
except Exception:
    kafka_api_key = "yourkey"

try:
    kafka_api_secret = spark.conf.get("kafka.api.secret")
except Exception:
    kafka_api_secret = "yoursecret"

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    f' required username="{kafka_api_key}" password="{kafka_api_secret}";'
)

out_rows = []
for r in orders_created:
    out_rows.append(Row(topic="orders.created", key=r.key, value=r.value))
for r in orders_updated:
    out_rows.append(Row(topic="orders.updated", key=r.key, value=r.value))
for r in payments:
    out_rows.append(Row(topic="payments.authorized", key=r.key, value=r.value))

if not out_rows:
    print("No rows to send; increase NUM_EVENTS or check random weights.")
else:
    df = spark.createDataFrame(out_rows)
    (
        df.write.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", jaas_config)
        .save()
    )
    print(f"Wrote {df.count()} messages to Kafka.")