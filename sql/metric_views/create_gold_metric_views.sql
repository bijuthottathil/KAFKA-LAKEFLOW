-- Unity Catalog metric views on DLT gold tables
-- Prerequisites: Databricks Runtime / SQL warehouse 17.2+ (YAML spec 1.1), SELECT on gold tables, CREATE TABLE on schema (see docs).
-- Replace catalog.schema below if yours differ from resources/kafka_lakeflow_pipeline.yml
--
-- Run in a SQL editor or as a job task after the pipeline has materialized gold tables at least once.
-- Docs: https://docs.databricks.com/aws/en/metric-views/create/sql

-- ---------------------------------------------------------------------------
-- 1) gold_orders_by_region — columns: region, order_count, total_revenue
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `na-dbxtraining`.biju_lakeflowschema.mv_gold_orders_by_region
COMMENT 'Semantic metrics on gold_orders_by_region (orders and revenue by region)'
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Order KPIs by region from DLT gold"
source: >
  SELECT * FROM `na-dbxtraining`.biju_lakeflowschema.gold_orders_by_region
dimensions:
  - name: region
    expr: region
measures:
  - name: order_count
    expr: SUM(order_count)
  - name: total_revenue
    expr: SUM(total_revenue)
$$;

-- ---------------------------------------------------------------------------
-- 2) gold_payments_by_method — columns: payment_method, payment_count, total_amount
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `na-dbxtraining`.biju_lakeflowschema.mv_gold_payments_by_method
COMMENT 'Semantic metrics on gold_payments_by_method'
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Payment totals by method from DLT gold"
source: >
  SELECT * FROM `na-dbxtraining`.biju_lakeflowschema.gold_payments_by_method
dimensions:
  - name: payment_method
    expr: payment_method
measures:
  - name: payment_count
    expr: SUM(payment_count)
  - name: total_amount
    expr: SUM(total_amount)
$$;

-- ---------------------------------------------------------------------------
-- 3) gold_event_counts_by_type — columns: event_type, event_count
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `na-dbxtraining`.biju_lakeflowschema.mv_gold_event_counts_by_type
COMMENT 'Semantic metrics on gold_event_counts_by_type'
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Event volume by type from DLT gold"
source: >
  SELECT * FROM `na-dbxtraining`.biju_lakeflowschema.gold_event_counts_by_type
dimensions:
  - name: event_type
    expr: event_type
measures:
  - name: event_count
    expr: SUM(event_count)
$$;

-- ---------------------------------------------------------------------------
-- 4) gold_order_status_history — SCD Type 2 (create_auto_cdc_flow)
--    System columns __START_AT / __END_AT: active row when __END_AT IS NULL
--    If your runtime uses different casing, adjust filter and expr (describe table first).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `na-dbxtraining`.biju_lakeflowschema.mv_gold_order_status_current
COMMENT 'Current order status only (SCD2 active rows: __END_AT IS NULL)'
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "One row per order for current status from gold_order_status_history"
source: >
  SELECT * FROM `na-dbxtraining`.biju_lakeflowschema.gold_order_status_history
filter: "`__END_AT` IS NULL"
dimensions:
  - name: order_id
    expr: order_id
  - name: new_status
    expr: new_status
  - name: status_effective_time
    expr: event_time
measures:
  - name: orders
    expr: COUNT(DISTINCT order_id)
$$;

CREATE OR REPLACE VIEW `na-dbxtraining`.biju_lakeflowschema.mv_gold_order_status_history
COMMENT 'Full status change history from gold_order_status_history (SCD2)'
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "All status versions per order over time"
source: >
  SELECT * FROM `na-dbxtraining`.biju_lakeflowschema.gold_order_status_history
dimensions:
  - name: order_id
    expr: order_id
  - name: new_status
    expr: new_status
  - name: event_time
    expr: event_time
  - name: is_current
    expr: "`__END_AT` IS NULL"
measures:
  - name: status_versions
    expr: COUNT(1)
$$;
