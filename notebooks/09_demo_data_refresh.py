# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 09 — Demo Data Refresh
# MAGIC
# MAGIC Refreshes time-sensitive objects so the Northwind Analytics catalog
# MAGIC always looks current when running a demo.
# MAGIC
# MAGIC ## What This Notebook Updates
# MAGIC
# MAGIC | Object | Action | Why |
# MAGIC |--------|--------|-----|
# MAGIC | `mart.agg_daily_sales` | INSERT OVERWRITE | Re-materialises with up-to-date aggregations |
# MAGIC | `ml.churn_predictions` | UPDATE scored_at | Timestamps look current (not from deployment day) |
# MAGIC | `ml.customer_features` | UPDATE snapshot_date | Snapshot date reflects current month |
# MAGIC | `mart.v_live_kpi` | CREATE OR REPLACE | Trailing-30-day KPI view from v_sales_current |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `00_main.py` must have completed successfully
# MAGIC - `08_genie_metadata_augmentation.py` must have run (`v_sales_current` must exist)
# MAGIC
# MAGIC ## Scheduling
# MAGIC Run this notebook as a daily **Databricks Workflow** job to keep the demo catalog current.
# MAGIC Estimated runtime: 2–5 minutes on a single-node cluster.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "sample_db", "Catalog Name")

CATALOG = dbutils.widgets.get("catalog_name")
DIM     = f"{CATALOG}.dimensions"
FACT    = f"{CATALOG}.facts"
MART    = f"{CATALOG}.mart"
ML      = f"{CATALOG}.ml"

def run_sql(stmt, ignore_errors=False):
    try:
        spark.sql(stmt)
    except Exception as e:
        if ignore_errors:
            print(f"  [WARN] {str(e)[:150]}")
        else:
            raise

print(f"Refreshing demo data in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md ## 1. Refresh agg_daily_sales
# MAGIC
# MAGIC Re-materialises the aggregated daily sales table. Because `v_sales_current`
# MAGIC is a live view, `agg_daily_sales` uses the original order dates (2022–2024).
# MAGIC This refresh ensures any new fact data added since deployment is captured.

# COMMAND ----------

import time

print("Refreshing mart.agg_daily_sales ...")
t0 = time.time()

run_sql(f"""
INSERT OVERWRITE {MART}.agg_daily_sales
SELECT
    d.full_date                                                        AS order_date,
    d.calendar_year                                                    AS year,
    d.calendar_quarter                                                 AS quarter,
    d.year_quarter,
    d.year_month,
    d.month_name,
    d.month_num,
    d.is_weekend,
    d.is_holiday,
    d.fiscal_year,
    d.fiscal_quarter,
    s.store_name,
    s.store_type,
    s.region                                                           AS store_region,
    s.country                                                          AS store_country,
    o.channel,
    COUNT(DISTINCT o.order_key)                                        AS num_orders,
    COUNT(DISTINCT o.customer_key)                                     AS unique_customers,
    SUM(o.total_amount)                                                AS total_revenue,
    SUM(o.subtotal)                                                    AS gross_revenue,
    SUM(o.discount_amount)                                             AS total_discounts,
    SUM(o.tax_amount)                                                  AS total_tax,
    AVG(o.total_amount)                                                AS avg_order_value,
    SUM(CASE WHEN o.is_returned = TRUE THEN o.total_amount ELSE 0 END) AS returned_revenue,
    SUM(CASE WHEN o.order_status = 'Cancelled' THEN 1 ELSE 0 END)    AS cancelled_orders,
    SUM(CASE WHEN o.promotion_key > 0 THEN 1 ELSE 0 END)             AS promoted_orders
FROM {FACT}.fact_orders      o
JOIN {DIM}.dim_date          d ON o.order_date_key = d.date_key
JOIN {DIM}.dim_store         s ON o.store_key      = s.store_key
WHERE o.order_status NOT IN ('Processing')
GROUP BY ALL
""")

row_count = spark.table(f"{MART}.agg_daily_sales").count()
print(f"  ✓  agg_daily_sales refreshed — {row_count:,} rows ({round(time.time()-t0, 1)}s)")

# COMMAND ----------

# MAGIC %md ## 2. Update ML Prediction Timestamps
# MAGIC
# MAGIC The `churn_predictions` table was scored at model deployment time.
# MAGIC Update `scored_at` to the current timestamp so demo audiences
# MAGIC see recent-looking predictions rather than month-old ones.

# COMMAND ----------

print("Updating ml.churn_predictions timestamps ...")

run_sql(f"""
UPDATE {ML}.churn_predictions
SET scored_at = current_timestamp()
""", ignore_errors=True)

pred_count = spark.table(f"{ML}.churn_predictions").count()
print(f"  ✓  churn_predictions: {pred_count:,} rows updated to scored_at = now()")

# COMMAND ----------

# MAGIC %md ## 3. Update Customer Feature Snapshot Date
# MAGIC
# MAGIC The `customer_features` snapshot_date was set to the deployment month.
# MAGIC Update to the current month so feature lookback windows look current.

# COMMAND ----------

print("Updating ml.customer_features snapshot_date ...")

run_sql(f"""
UPDATE {ML}.customer_features
SET snapshot_date = date_trunc('month', current_date())
""", ignore_errors=True)

feat_count = spark.table(f"{ML}.customer_features").count()
print(f"  ✓  customer_features: {feat_count:,} rows updated to snapshot_date = current month")

# COMMAND ----------

# MAGIC %md ## 4. v_live_kpi — Trailing-30-Day KPI View
# MAGIC
# MAGIC A lightweight view computing KPIs over the trailing 30 days using `v_sales_current`.
# MAGIC Because `v_sales_current` shifts all dates to be relative to today, this view
# MAGIC always returns "last 30 days" data regardless of when the demo runs.
# MAGIC
# MAGIC Use in Genie for questions like:
# MAGIC - *"What is our revenue for the last 30 days?"*
# MAGIC - *"How many orders did we process this week?"*
# MAGIC - *"What is the daily revenue trend for the past month?"*

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE VIEW {MART}.v_live_kpi
COMMENT 'Trailing-30-day KPI metrics computed in real time from v_sales_current.
Because v_sales_current shifts all dates to be relative to today, this view always reflects
the most recent 30 days of data regardless of when the demo catalog was deployed.
Best for: "what is revenue for the last month", "how many orders this week", "daily revenue trend".
Refresh frequency: real-time (no materialisation).'
AS
SELECT
    order_date,
    store_name,
    store_region,
    channel,
    num_orders,
    num_unique_customers,
    net_revenue,
    gross_revenue,
    total_discounts,
    avg_order_value,
    num_returned_orders,
    returned_revenue,
    num_cancelled_orders,
    num_promoted_orders,
    DATEDIFF(current_date(), order_date)                               AS days_ago,
    SUM(net_revenue) OVER (ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     AS rolling_7d_revenue,
    SUM(num_orders)  OVER (ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                     AS rolling_7d_orders
FROM {MART}.v_sales_current
WHERE order_date >= date_sub(current_date(), 30)
""")

print("v_live_kpi created")

# COMMAND ----------

# MAGIC %md ## 5. Validation

# COMMAND ----------

checks = []

# Check agg_daily_sales row count
agg_count = spark.table(f"{MART}.agg_daily_sales").count()
checks.append(("agg_daily_sales row count", agg_count >= 100, f"{agg_count:,} rows"))

# Check v_live_kpi returns rows
live_count = spark.table(f"{MART}.v_live_kpi").count()
checks.append(("v_live_kpi returns rows", live_count >= 1, f"{live_count:,} rows (last 30 days)"))

# Check churn_predictions scored_at is recent (within last hour)
from datetime import datetime, timedelta
recent_preds = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {ML}.churn_predictions
    WHERE scored_at >= current_timestamp() - INTERVAL 1 HOUR
""").collect()[0]["cnt"]
checks.append(("churn_predictions recently updated", recent_preds > 0, f"{recent_preds:,} rows with recent timestamp"))

# Check customer_features snapshot_date is current month
current_features = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {ML}.customer_features
    WHERE snapshot_date = date_trunc('month', current_date())
""").collect()[0]["cnt"]
checks.append(("customer_features snapshot current", current_features > 0, f"{current_features:,} rows with current snapshot"))

print("\nValidation results:")
all_passed = True
for name, passed, detail in checks:
    status = "✓" if passed else "✗"
    print(f"  {status}  {name:<45} {detail}")
    if not passed:
        all_passed = False

if not all_passed:
    raise Exception("One or more validation checks failed — see output above.")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("=" * 60)
print("  Demo Data Refresh — Complete")
print("=" * 60)
print(f"  ✓  agg_daily_sales refreshed      {agg_count:>10,} rows")
print(f"  ✓  churn_predictions updated       {pred_count:>10,} rows")
print(f"  ✓  customer_features updated       {feat_count:>10,} rows")
print(f"  ✓  v_live_kpi created              {live_count:>10,} rows (last 30d)")
print()
print("  Your Genie Space is ready. Suggested opening questions:")
print("  → 'Give me a KPI summary for the business'")
print("  → 'What was revenue last week vs the week before?'")
print("  → 'How many customers are at churn risk right now?'")
print("  → 'Which stores are underperforming their monthly targets?'")
print("=" * 60)
