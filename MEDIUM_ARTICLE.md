# From Kafka to the Lakehouse: Building a Medallion Pipeline with Confluent Cloud and Databricks

*A practical walkthrough of streaming orders and payments into Delta Live Tables—bronze, silver, gold—and shipping it all with Databricks Asset Bundles.*

---

## Why this project exists

Teams moving to a **lakehouse** still need a reliable way to land **real-time events**. **Apache Kafka** is the usual backbone; **Databricks** gives you governed tables, SQL, and ML on top of **Delta**. The gap is often glue code: authentication to a managed Kafka (here **Confluent Cloud**), a clear **medallion** layout, and a path to **deploy** the same pipeline across workspaces without copy-pasting notebooks.

This article accompanies an open reference implementation that wires those pieces together. The goal is not a toy “hello world,” but a pattern you can adapt: **multi-topic** consumption, **incremental** bronze ingestion, **typed** silver parsing, and **gold** aggregates your analysts can query.

---

## What we’re building

Picture three Kafka topics:

- `orders.created` — new orders with line items and totals  
- `orders.updated` — status changes  
- `payments.authorized` — payment confirmation tied to an order  

Events flow into **Delta Live Tables (DLT)** in **Unity Catalog**:

| Layer | Table (example) | Role |
|--------|------------------|------|
| Bronze | `raw_kafka_orders` | Immutable-style raw stream: key, value, topic, offset, timestamps |
| Silver | `silver_order_events` | Parsed JSON, one logical schema across event types |
| Gold | e.g. revenue by region, payments by method | Materialized views for analytics and monitoring |

A small **PySpark notebook** can simulate producers so you can test the pipeline without an external order system.

The name **“Lakeflow”** here is shorthand: **lake** (Delta as the system of record) plus **flow** (continuous or triggered processing over streams).

---

## Architecture at a glance

```mermaid
flowchart LR
  subgraph confluent [Confluent Cloud]
    T1[orders.created]
    T2[orders.updated]
    T3[payments.authorized]
  end
  subgraph databricks [Databricks]
    B[Bronze]
    S[Silver]
    G[Gold]
  end
  T1 --> B
  T2 --> B
  T3 --> B
  B --> S
  S --> G
```

**Bronze** uses Spark’s Kafka source with **SASL_SSL** and **PLAIN** (API key and secret from Confluent), and subscribes to a **comma-separated** topic list—one reader, three topics. **Silver** uses a single **JSON schema** with optional fields so `order.created`, `order.updated`, and `payment.authorized` payloads all map cleanly. **Gold** filters on `event_type` and builds the metrics you care about.

---

## Stack and prerequisites

- **Confluent Cloud** cluster and API key with access to your topics  
- **Databricks** workspace with **Unity Catalog**, **DLT**, and permission to run **serverless** pipelines (as configured in the project)  
- **Databricks CLI** (≥ 0.279.0) — deploy with **`DATABRICKS_BUNDLE_ENGINE=direct`** and **`databricks bundle deploy`** (direct deployment engine)  
- Familiarity with **PySpark** and the **medallion** idea helps but isn’t mandatory  

Store **secrets** in **Databricks Secrets** or your CI secret store for anything beyond a personal sandbox—never treat API keys as permanent documentation.

---

## How the repository is organized

The project uses a **Databricks Asset Bundle**: `databricks.yml` points at your workspace and sync path; `resources/kafka_lakeflow_pipeline.yml` defines the **DLT pipeline** (catalog, schema, Photon/serverless, Kafka configuration, library glob).

Python modules under `src/` run in order (`01` → `02` → `03`):

1. **Bronze** — `readStream` from Kafka, append flow into Delta  
2. **Silver** — `from_json` into a unified struct, expectations on core fields  
3. **Gold** — materialized views for regional revenue, payment mix, event counts, latest status  

The simulator lives under `notebooks/` and writes to Kafka using the same bootstrap and SASL settings you pass through Spark configuration.

For a **field-by-field** breakdown of `databricks.yml` and Kafka keys, see the project’s main `README.md`.

---

## Deployment in one minute

After configuring your workspace profile and `root_path` in `databricks.yml`:

```bash
databricks bundle validate -t dev
DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy -t dev
databricks bundle summary -t dev
```

If local bundle cache causes deploy errors, run **in order** (from the repo root; adjust `dev` if needed):

```bash
rm -rf .databricks/bundle/dev
DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy -t dev
databricks bundle summary -t dev
```

That syncs sources and updates the pipeline definition. Run the pipeline from the Databricks UI, or trigger it on a schedule once you’re happy with cost and latency.

---

## Lessons worth taking away

1. **Multi-topic subscribe** keeps operational overhead low—one checkpointed stream, topic preserved in the bronze table for lineage.  
2. **One silver schema with optional fields** often beats three separate streams when event volumes are moderate and schemas are related.  
3. **Bundles** turn “works on my machine” into “works in our workspace” with reviewable YAML and repeatable deploys.  
4. **Simulators** that mirror production JSON save hours when Kafka ACLs and producers are owned by another team.  

---

## Closing

Streaming from Kafka into governed Delta tables is a standard pattern; the details—auth, topic lists, DLT expectations, Unity Catalog targets—are where projects succeed or stall. This reference ties them together so you can focus on **business logic** in silver and gold instead of plumbing.

**Repository:** [github.com/bijuthottathil/KAFKA-LAKEFLOW](https://github.com/bijuthottathil/KAFKA-LAKEFLOW)

---

## Notes for publishing on Medium

- Medium does not render **Mermaid** in all contexts; export the diagram as a PNG from [Mermaid Live](https://mermaid.live) or duplicate the flow in a simple bullet list for the imported post.  
- Paste code blocks with **fixed-width font** and keep lines short for mobile readers.  
- Add a **featured image** (architecture sketch or Confluent + Databricks logos per each vendor’s brand guidelines).  
- Replace or remove the GitHub URL if you mirror the repo elsewhere.  
- Consider a **TL;DR** bullet list at the top for scroll-heavy readers.
