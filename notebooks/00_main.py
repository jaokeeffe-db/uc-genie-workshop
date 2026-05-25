# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # Northwind Analytics — Sample Retail Database Deployment
# MAGIC
# MAGIC This notebook deploys a **comprehensive sample retail database** to Databricks Unity Catalog,
# MAGIC designed to showcase catalog management, governance, search, lineage, and Genie (AI/BI) capabilities.
# MAGIC
# MAGIC ## What Gets Created
# MAGIC
# MAGIC | Schema | Objects | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `dimensions` | 6 tables | Customer, product, store, employee, date, promotion |
# MAGIC | `facts` | 4 tables | Orders, order items, inventory snapshots, returns |
# MAGIC | `mart` | 6 views + 1 aggregate table + 4 metric views | Business-facing analytics, Genie-optimised |
# MAGIC | `ml` | 2 feature tables + 1 registered model | Churn prediction feature store and UC model |
# MAGIC | `raw` | 1 managed volume + sample files | File landing zone for raw CSV/JSON uploads |
# MAGIC
# MAGIC ## Supplementary Notebooks (Genie Demo Optimisation)
# MAGIC
# MAGIC Run these after the core deployment to maximise Genie (AI/BI) demo effectiveness:
# MAGIC
# MAGIC | Notebook | Purpose |
# MAGIC |----------|---------|
# MAGIC | `08_genie_metadata_augmentation` | Adds `v_sales_current` (date-shifted), `v_kpi_executive`, `metric_definitions` table, and enhanced Genie tags |
# MAGIC | `09_demo_data_refresh` | Refreshes `agg_daily_sales`, updates ML timestamps, creates `v_live_kpi` for trailing-30-day queries |
| `11_metric_views` | Creates `mv_sales`, `mv_customers`, `mv_products`, `mv_stores` metric views — semantic layer for Genie |
# MAGIC
# MAGIC See **`IMPROVEMENTS.md`** for the full improvement plan, bug list, and Genie demo script.
# MAGIC
# MAGIC ## Unity Catalog Features Showcased
# MAGIC - **Tags** — catalog, schema, table, and column-level tags for search and classification
# MAGIC - **Comments** — rich descriptions on every object and column for Genie and data discovery
# MAGIC - **Liquid Clustering** — modern Delta Lake clustering on all large tables
# MAGIC - **Row Filters** — dynamic row-level security based on Unity Catalog group membership
# MAGIC - **Column Masks** — PII masking for email, phone, and cost columns
# MAGIC - **Primary / Foreign Keys** — informational constraints for query optimisation
# MAGIC - **Managed Volume** — Unity Catalog managed storage for raw file ingestion
# MAGIC - **Model Registry** — ML model registered and aliased in Unity Catalog
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog-enabled Databricks workspace (UC metastore attached)
# MAGIC - Cluster running **DBR 14.3 LTS+** or **ML Runtime 14.3+**
# MAGIC - `CREATE CATALOG` privilege, or provide an existing catalog name and ensure `CREATE SCHEMA` is granted
# MAGIC - All notebooks imported to the **same workspace folder**
# MAGIC
# MAGIC ## Deployment
# MAGIC 1. Configure the widget values below
# MAGIC 2. Click **Run All** (estimated: 10–20 minutes)
# MAGIC 3. Set `reset_catalog = true` to drop and fully recreate the catalog
# MAGIC
# MAGIC ## Post-Deployment: Genie Setup
# MAGIC After deployment, create a Genie Space using tables from the `mart` schema.
# MAGIC Suggested starter questions are documented in `04_analytics_views`.

# COMMAND ----------

try:
    dbutils.widgets.removeAll()
except Exception:
    pass

dbutils.widgets.text("catalog_name",             "sample_db", "Catalog Name")
dbutils.widgets.text("catalog_managed_location", "",          "Catalog managed location (optional)")
dbutils.widgets.text("env",           "dev",        "Environment (dev / prod)")
dbutils.widgets.text("reset_catalog", "false",      "Drop & recreate catalog? (true / false)")
dbutils.widgets.text("num_customers", "2000",       "Number of customers to generate")
dbutils.widgets.text("num_orders",    "50000",      "Number of orders to generate")

# COMMAND ----------

import time

CATALOG              = dbutils.widgets.get("catalog_name")
CATALOG_MANAGED_LOC  = dbutils.widgets.get("catalog_managed_location")
ENV                  = dbutils.widgets.get("env")
RESET                = dbutils.widgets.get("reset_catalog")
NUM_CUSTOMERS        = dbutils.widgets.get("num_customers")
NUM_ORDERS           = dbutils.widgets.get("num_orders")

PARAMS = {
    "catalog_name":  CATALOG,
    "env":           ENV,
    "reset_catalog": RESET,
    "num_customers": NUM_CUSTOMERS,
    "num_orders":    NUM_ORDERS,
}

print("=" * 65)
print("  Northwind Analytics — Sample Retail Database Deployment")
print("=" * 65)
print(f"  Catalog     : {CATALOG}")
if CATALOG_MANAGED_LOC.strip():
    print(f"  Managed loc : {CATALOG_MANAGED_LOC.strip()}")
print(f"  Environment : {ENV}")
print(f"  Reset       : {RESET}")
print(f"  Customers   : {NUM_CUSTOMERS}")
print(f"  Orders      : {NUM_ORDERS}")
print("=" * 65)

# COMMAND ----------

# MAGIC %md ## Deployment Steps

# COMMAND ----------

STEPS = [
    ("01_catalog_setup",             "Catalog, schemas, tags, managed volume",           600),
    ("02_dimension_tables",          "Dimension tables — customer, product, store …",    900),
    ("03_fact_tables",               "Fact tables — orders, items, inventory, returns",  1800),
    ("04_analytics_views",           "Mart schema — views and aggregate tables",         600),
    ("05_ml_models",                 "Feature engineering and ML model registration",    1200),
    ("06_governance",                "Row filters, column masks, comments, PKs/FKs",    600),
    ("07_validate",                  "Row-count and referential integrity validation",   300),
    # Supplementary steps — Genie optimisation (see IMPROVEMENTS.md)
    ("08_genie_metadata_augmentation", "Genie views, metric glossary, enhanced tags",   600),
    ("09_demo_data_refresh",           "Refresh agg tables and ML timestamps for demo", 300),
    ("11_metric_views",                "Metric views — semantic layer for Genie AI/BI", 300),
]

results   = []
all_ok    = True
run_start = time.time()

for notebook, description, timeout_s in STEPS:
    step_start = time.time()
    status = "SUCCESS"
    error  = None
    try:
        print(f"\n▶  {notebook}: {description}")
        run_args = dict(PARAMS)
        if notebook == "01_catalog_setup":
            run_args["catalog_managed_location"] = CATALOG_MANAGED_LOC
        dbutils.notebook.run(f"./{notebook}", timeout_seconds=timeout_s, arguments=run_args)
        duration = round(time.time() - step_start, 1)
        print(f"   ✓  Done ({duration}s)")
    except Exception as exc:
        duration = round(time.time() - step_start, 1)
        status   = "FAILED"
        error    = str(exc)[:300]
        all_ok   = False
        print(f"   ✗  FAILED after {duration}s\n   {error}")
        raise  # halt on first failure to surface root cause clearly

    results.append({
        "step":        notebook,
        "description": description,
        "status":      status,
        "duration_s":  duration,
        "error":       error if error else "",
    })

total_s = round(time.time() - run_start, 1)
print(f"\n{'=' * 65}")
if all_ok:
    print(f"  Deployment complete in {total_s}s")
    print(f"  Explore your catalog: Data Explorer → {CATALOG}")
else:
    print(f"  Deployment FAILED after {total_s}s — see errors above")
print(f"{'=' * 65}")

# COMMAND ----------

display(spark.createDataFrame(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick-Start Queries
# MAGIC
# MAGIC Paste these into the **SQL Editor** (replace `sample_db` with your catalog name):
# MAGIC
# MAGIC ```sql
# MAGIC -- Top 10 stores by revenue in 2024
# MAGIC SELECT store_name, store_region, SUM(net_revenue) AS revenue
# MAGIC FROM sample_db.mart.v_sales_summary
# MAGIC WHERE year = 2024
# MAGIC GROUP BY store_name, store_region
# MAGIC ORDER BY revenue DESC LIMIT 10;
# MAGIC
# MAGIC -- Monthly revenue trend with YoY comparison
# MAGIC SELECT year, month_name, month_num,
# MAGIC        SUM(net_revenue) AS revenue,
# MAGIC        LAG(SUM(net_revenue), 12) OVER (ORDER BY year, month_num) AS prev_year_revenue
# MAGIC FROM sample_db.mart.v_sales_summary
# MAGIC GROUP BY year, month_name, month_num ORDER BY year, month_num;
# MAGIC
# MAGIC -- Customers at churn risk with high lifetime value
# MAGIC SELECT customer_id, first_name, last_name, loyalty_tier,
# MAGIC        lifetime_value, days_since_last_order
# MAGIC FROM sample_db.mart.v_customer_360
# MAGIC WHERE is_churn_risk = TRUE AND lifetime_value > 1000
# MAGIC ORDER BY lifetime_value DESC LIMIT 25;
# MAGIC
# MAGIC -- Product return rates by category
# MAGIC SELECT category, sub_category,
# MAGIC        SUM(units_sold) AS units_sold,
# MAGIC        SUM(return_quantity) AS returns,
# MAGIC        ROUND(SUM(return_quantity) / NULLIF(SUM(units_sold), 0) * 100, 2) AS return_rate_pct
# MAGIC FROM sample_db.mart.v_product_performance
# MAGIC GROUP BY category, sub_category ORDER BY return_rate_pct DESC;
# MAGIC ```
# MAGIC
# MAGIC ## Suggested Genie Space Setup
# MAGIC
# MAGIC 1. Navigate to **Genie** in the left sidebar → **New Genie Space**
# MAGIC 2. Name it **"Northwind Retail Analytics"**
# MAGIC 3. Add these tables from the `mart` schema:
# MAGIC    - `v_sales_summary` — daily revenue by store, segment, and channel
# MAGIC    - `v_customer_360` — customer lifetime value, recency, and churn risk
# MAGIC    - `v_product_performance` — sales, returns, and margin by product
# MAGIC    - `v_store_performance` — store ranking and targets
# MAGIC    - `agg_daily_sales` — pre-aggregated daily totals for fast trend queries
# MAGIC 4. Add these as **Trusted Assets** (example SQL):
# MAGIC    - Total revenue by quarter
# MAGIC    - Top 10 products by revenue
# MAGIC    - Churn risk customers
# MAGIC 5. Try natural-language questions:
# MAGIC    - *"What was total revenue last quarter?"*
# MAGIC    - *"Which stores are underperforming their targets?"*
# MAGIC    - *"Show me the top 5 customers by lifetime value"*
# MAGIC    - *"What is the return rate for electronics products?"*
# MAGIC    - *"Compare Q4 2023 vs Q4 2024 revenue by region"*
