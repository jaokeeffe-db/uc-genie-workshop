# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 — Genie Metadata Augmentation
# MAGIC
# MAGIC Enhances the Northwind Analytics catalog with additional objects and metadata
# MAGIC specifically designed to make **Databricks Genie (AI/BI)** more effective.
# MAGIC
# MAGIC ## What This Notebook Creates
# MAGIC
# MAGIC | Object | Type | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | `mart.v_sales_current` | View | Date-shifted sales view so "this month" queries return data |
# MAGIC | `mart.v_kpi_executive` | View | Single-row executive KPI summary for demo opener questions |
# MAGIC | `mart.metric_definitions` | Delta Table | Business metric glossary — grounds Genie in domain terminology |
# MAGIC | Enhanced comments | Metadata | Richer column descriptions on `agg_daily_sales` and `v_customer_360` |
# MAGIC | Genie tags | Tags | `genie_primary_filters`, `genie_measures`, `refresh_frequency` tags on mart objects |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `00_main.py` must have completed successfully
# MAGIC - Run on **DBR 14.3 LTS+**

# COMMAND ----------

dbutils.widgets.text("catalog_name", "sample_db", "Catalog Name")

CATALOG = dbutils.widgets.get("catalog_name")
DIM     = f"{CATALOG}.dimensions"
FACT    = f"{CATALOG}.facts"
MART    = f"{CATALOG}.mart"
ML      = f"{CATALOG}.ml"

def run_sql(stmt, ignore_errors=False):
    """Execute SQL with optional error suppression for idempotency."""
    try:
        spark.sql(stmt)
    except Exception as e:
        if ignore_errors:
            print(f"  [WARN] {str(e)[:150]}")
        else:
            raise

print(f"Augmenting Genie metadata in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md ## 1. v_sales_current — Date-Shifted Sales View
# MAGIC
# MAGIC The fact tables contain order data from 2022-01-01 to 2024-12-31.
# MAGIC When a demo runs in 2025 or 2026, questions like *"What was revenue this month?"*
# MAGIC return empty results because no data exists for the current date.
# MAGIC
# MAGIC This view shifts all dates forward by `DATEDIFF(current_date(), max_order_date)`
# MAGIC so the most recent data always appears as "this month / this quarter / this year."
# MAGIC
# MAGIC **Add this view to your Genie Space alongside `v_sales_summary`.**

# COMMAND ----------

# Calculate the date offset: how many days to shift all dates forward
max_date_row = spark.sql(f"""
    SELECT MAX(d.full_date) AS max_date
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key
""").collect()[0]

max_order_date = str(max_date_row["max_date"])
print(f"Latest order date in fact_orders: {max_order_date}")
print(f"Current date: {spark.sql('SELECT current_date() AS d').collect()[0]['d']}")

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE VIEW {MART}.v_sales_current
COMMENT 'Date-shifted version of v_sales_summary. All dates are shifted forward so the most recent
data in the warehouse always appears as the current period. Use this view (not v_sales_summary)
when answering questions about "this month", "this quarter", "this year", or "recent" performance.
The underlying data comes from fact_orders; the date offset is computed dynamically as
DATEDIFF(current_date(), latest_order_date). Column definitions are identical to v_sales_summary.'
AS
WITH date_offset AS (
    SELECT DATEDIFF(current_date(), MAX(d.full_date)) AS offset_days
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key
),
shifted_orders AS (
    SELECT
        o.*,
        date_add(d.full_date, (SELECT offset_days FROM date_offset))         AS shifted_date,
        YEAR(date_add(d.full_date, (SELECT offset_days FROM date_offset)))    AS shifted_year,
        QUARTER(date_add(d.full_date, (SELECT offset_days FROM date_offset))) AS shifted_quarter,
        MONTH(date_add(d.full_date, (SELECT offset_days FROM date_offset)))   AS shifted_month,
        DAYOFWEEK(date_add(d.full_date, (SELECT offset_days FROM date_offset))) AS shifted_dow,
        date_format(date_add(d.full_date, (SELECT offset_days FROM date_offset)), 'MMMM') AS shifted_month_name,
        date_format(date_add(d.full_date, (SELECT offset_days FROM date_offset)), 'EEEE') AS shifted_day_name,
        CONCAT(YEAR(date_add(d.full_date, (SELECT offset_days FROM date_offset))), '-Q',
               QUARTER(date_add(d.full_date, (SELECT offset_days FROM date_offset))))     AS shifted_year_quarter,
        CONCAT(YEAR(date_add(d.full_date, (SELECT offset_days FROM date_offset))), '-',
               LPAD(MONTH(date_add(d.full_date, (SELECT offset_days FROM date_offset))), 2, '0')) AS shifted_year_month,
        d.is_weekend,
        d.is_holiday,
        d.fiscal_year,
        d.fiscal_quarter
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key
    WHERE o.order_status NOT IN ('Processing')
)
SELECT
    so.shifted_date                                                     AS order_date,
    so.shifted_year                                                     AS year,
    so.shifted_quarter                                                  AS quarter,
    so.shifted_year_quarter                                             AS year_quarter,
    so.shifted_year_month                                               AS year_month,
    so.shifted_month_name                                               AS month_name,
    so.shifted_month                                                    AS month_num,
    so.shifted_day_name                                                 AS day_name,
    so.is_weekend,
    so.is_holiday,
    so.fiscal_year,
    so.fiscal_quarter,
    s.store_id,
    s.store_name,
    s.store_type,
    s.city                                                              AS store_city,
    s.state_province                                                    AS store_state,
    s.country                                                           AS store_country,
    s.region                                                            AS store_region,
    c.segment                                                           AS customer_segment,
    c.loyalty_tier                                                      AS customer_loyalty_tier,
    c.country                                                           AS customer_country,
    so.channel,
    so.payment_method,
    so.shipping_method,
    COUNT(so.order_key)                                                 AS num_orders,
    COUNT(DISTINCT so.customer_key)                                     AS num_unique_customers,
    SUM(so.subtotal)                                                    AS gross_revenue,
    SUM(so.discount_amount)                                             AS total_discounts,
    SUM(so.tax_amount)                                                  AS total_tax,
    SUM(so.shipping_amount)                                             AS total_shipping,
    SUM(so.total_amount)                                                AS net_revenue,
    AVG(so.total_amount)                                                AS avg_order_value,
    MIN(so.total_amount)                                                AS min_order_value,
    MAX(so.total_amount)                                                AS max_order_value,
    SUM(CASE WHEN so.is_returned = TRUE THEN 1 ELSE 0 END)             AS num_returned_orders,
    SUM(CASE WHEN so.is_returned = TRUE THEN so.total_amount ELSE 0 END) AS returned_revenue,
    SUM(CASE WHEN so.order_status = 'Cancelled' THEN 1 ELSE 0 END)    AS num_cancelled_orders,
    SUM(CASE WHEN so.promotion_key > 0 THEN 1 ELSE 0 END)             AS num_promoted_orders
FROM shifted_orders so
JOIN {DIM}.dim_store    s ON so.store_key    = s.store_key
JOIN {DIM}.dim_customer c ON so.customer_key = c.customer_key
GROUP BY ALL
""")

print("v_sales_current created")

# COMMAND ----------

# MAGIC %md ## 2. v_kpi_executive — Single-Row Executive KPI Summary
# MAGIC
# MAGIC A single-row view giving Genie a ready-made answer for questions like
# MAGIC *"Give me a business summary"* or *"What are our key KPIs?"*.
# MAGIC
# MAGIC Uses `v_sales_current` so metrics always reflect the current period.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE VIEW {MART}.v_kpi_executive
COMMENT 'Single-row executive KPI summary using date-shifted data (v_sales_current).
All period comparisons are relative to today: MTD = current month-to-date,
QTD = current quarter-to-date, YTD = current year-to-date.
Prior period comparisons use the same calendar period one year ago.
Best for questions like: give me a business summary, what are our top KPIs, how are we tracking.
Refresh frequency: real-time (computed on query).'
AS
WITH current_periods AS (
    SELECT
        YEAR(current_date())    AS cur_year,
        MONTH(current_date())   AS cur_month,
        QUARTER(current_date()) AS cur_quarter
),
ytd AS (
    SELECT SUM(net_revenue) AS revenue, COUNT(num_orders) AS orders, SUM(num_unique_customers) AS customers
    FROM {MART}.v_sales_current
    WHERE year = (SELECT cur_year FROM current_periods)
),
ytd_prior AS (
    SELECT SUM(net_revenue) AS revenue
    FROM {MART}.v_sales_current
    WHERE year = (SELECT cur_year FROM current_periods) - 1
      AND month_num <= (SELECT cur_month FROM current_periods)
),
qtd AS (
    SELECT SUM(net_revenue) AS revenue, COUNT(num_orders) AS orders
    FROM {MART}.v_sales_current
    WHERE year    = (SELECT cur_year    FROM current_periods)
      AND quarter = (SELECT cur_quarter FROM current_periods)
),
qtd_prior AS (
    SELECT SUM(net_revenue) AS revenue
    FROM {MART}.v_sales_current
    WHERE year    = (SELECT cur_year    FROM current_periods) - 1
      AND quarter = (SELECT cur_quarter FROM current_periods)
),
mtd AS (
    SELECT SUM(net_revenue) AS revenue, AVG(avg_order_value) AS avg_order_value, COUNT(num_orders) AS orders
    FROM {MART}.v_sales_current
    WHERE year     = (SELECT cur_year    FROM current_periods)
      AND month_num = (SELECT cur_month FROM current_periods)
),
churn_stats AS (
    SELECT
        COUNT(*)                               AS churn_risk_customers,
        SUM(lifetime_value)                    AS revenue_at_risk,
        AVG(days_since_last_order)             AS avg_days_inactive
    FROM {MART}.v_customer_360
    WHERE is_churn_risk = TRUE
),
top_store AS (
    SELECT store_name, SUM(actual_revenue) AS revenue
    FROM {MART}.v_store_performance
    WHERE year = (SELECT cur_year - 1 FROM current_periods)   -- last full year
    GROUP BY store_name
    ORDER BY revenue DESC
    LIMIT 1
),
return_stats AS (
    SELECT
        SUM(num_returned_orders) / NULLIF(SUM(num_orders), 0) * 100 AS overall_return_rate_pct
    FROM {MART}.v_sales_current
    WHERE year = (SELECT cur_year FROM current_periods)
)
SELECT
    current_date()                                                      AS report_date,
    ROUND(ytd.revenue, 2)                                               AS ytd_net_revenue,
    ROUND(ytd_prior.revenue, 2)                                         AS ytd_prior_year_revenue,
    ROUND((ytd.revenue - ytd_prior.revenue)
          / NULLIF(ytd_prior.revenue, 0) * 100, 1)                     AS ytd_growth_pct,
    ROUND(qtd.revenue, 2)                                               AS qtd_net_revenue,
    ROUND(qtd_prior.revenue, 2)                                         AS qtd_prior_year_revenue,
    ROUND((qtd.revenue - qtd_prior.revenue)
          / NULLIF(qtd_prior.revenue, 0) * 100, 1)                     AS qtd_growth_pct,
    ROUND(mtd.revenue, 2)                                               AS mtd_net_revenue,
    ROUND(mtd.avg_order_value, 2)                                       AS mtd_avg_order_value,
    ROUND(churn_stats.churn_risk_customers, 0)                          AS customers_at_churn_risk,
    ROUND(churn_stats.revenue_at_risk, 2)                               AS lifetime_revenue_at_risk,
    ROUND(churn_stats.avg_days_inactive, 0)                             AS avg_days_inactive_churn_customers,
    top_store.store_name                                                AS top_store_by_revenue,
    ROUND(return_stats.overall_return_rate_pct, 2)                      AS ytd_return_rate_pct
FROM ytd, ytd_prior, qtd, qtd_prior, mtd, churn_stats, top_store, return_stats
""")

print("v_kpi_executive created")

# COMMAND ----------

# MAGIC %md ## 3. metric_definitions — Business Metric Glossary
# MAGIC
# MAGIC A lookup table defining every key metric used across the mart schema.
# MAGIC Genie can query this when users ask "what does X mean?" and it also helps
# MAGIC catalog users discover available metrics.

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

metrics = [
    # Revenue metrics
    ("gross_revenue",          "Gross Revenue",               "Total order value before any discounts are applied.",
     "SUM(subtotal)",          "mart.v_sales_summary",        "What was gross revenue last quarter?"),
    ("net_revenue",            "Net Revenue",                 "Revenue after discounts but before tax and shipping. The primary top-line metric.",
     "SUM(total_amount)",      "mart.v_sales_summary",        "Show net revenue by region for last year."),
    ("total_discounts",        "Total Discounts",             "Sum of all discount amounts applied to orders. High values indicate heavy promotional activity.",
     "SUM(discount_amount)",   "mart.v_sales_summary",        "What percentage of revenue came from discounts?"),
    ("avg_order_value",        "Average Order Value (AOV)",   "Mean order total. Benchmark: healthy AOV for this business is $100–$150.",
     "AVG(total_amount)",      "mart.v_sales_summary",        "What is the average order value this month?"),
    ("revenue_vs_target",      "Revenue vs Target",           "Monthly actual revenue minus the prorated annual store target. Negative = underperforming.",
     "actual_revenue - (annual_target / 12)", "mart.v_store_performance", "Which stores are underperforming their targets?"),
    ("pct_of_target",          "% of Target",                "Actual revenue as a percentage of the prorated monthly target. 100% = on target.",
     "actual_revenue / (annual_target / 12) * 100", "mart.v_store_performance", "Show store target attainment this year."),
    # Customer metrics
    ("lifetime_value",         "Customer Lifetime Value (LTV)","Sum of all order totals for a customer since registration. VIP threshold: $5,000.",
     "SUM(total_amount) per customer", "mart.v_customer_360", "Who are the top 10 customers by lifetime value?"),
    ("is_churn_risk",          "Churn Risk Flag",             "TRUE when a customer has not ordered in more than 60 days AND had fewer than 2 orders in the last 90 days.",
     "days_since_last_order > 60 AND orders_last_90d < 2", "mart.v_customer_360", "How many customers are at churn risk?"),
    ("retention_rate_pct",     "Cohort Retention Rate",       "Percentage of customers from an acquisition cohort who placed at least one order in a given period. period_number=0 is always 100%.",
     "active_customers / cohort_size * 100", "mart.v_cohort_analysis", "What is the 3-month retention rate for 2023 cohorts?"),
    ("days_since_last_order",  "Days Since Last Order",       "Number of days since the customer's most recent delivered order. Customers with 60+ days are flagged as churn risk.",
     "DATEDIFF(current_date(), MAX(order_date))", "mart.v_customer_360", "Show customers who haven't ordered in 90 days."),
    # Product metrics
    ("gross_margin_pct",       "Gross Margin %",              "Gross profit as a percentage of revenue. Formula: (revenue - cost) / revenue * 100. Finance role required to see unit_cost.",
     "(revenue - total_cost) / revenue * 100", "mart.v_product_performance", "What is the gross margin for Electronics?"),
    ("return_rate_pct",        "Return Rate %",               "Returns as a percentage of units sold. Values above 15% indicate potential quality or fulfilment issues.",
     "return_quantity / units_sold * 100", "mart.v_product_performance", "Which categories have the highest return rates?"),
    # Operational
    ("revenue_per_sqft",       "Revenue per Sq Ft",           "Monthly store revenue divided by floor area in square feet. Key retail efficiency metric.",
     "actual_revenue / floor_area_sqft", "mart.v_store_performance", "Which stores have the highest revenue per square foot?"),
    ("revenue_per_employee",   "Revenue per Employee",        "Monthly store revenue divided by number of employees. Measures staff productivity.",
     "actual_revenue / num_employees", "mart.v_store_performance", "Which region generates the most revenue per employee?"),
    ("churn_probability",      "Churn Probability",           "Score 0.0–1.0 from the churn_predictor ML model. Risk tiers: Low (<0.3), Medium (0.3–0.5), High (0.5–0.7), Critical (>0.7).",
     "ML model output",        "ml.churn_predictions",        "Show customers with a churn probability above 0.7."),
]

schema = StructType([
    StructField("metric_name",        StringType(), True),
    StructField("friendly_name",      StringType(), True),
    StructField("definition",         StringType(), True),
    StructField("formula",            StringType(), True),
    StructField("source_table",       StringType(), True),
    StructField("example_question",   StringType(), True),
])

df = spark.createDataFrame(metrics, schema=schema)

df.write.format("delta").mode("overwrite").saveAsTable(f"{MART}.metric_definitions")

spark.sql(f"""
ALTER TABLE {MART}.metric_definitions
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
)
""")

run_sql(f"""
COMMENT ON TABLE {MART}.metric_definitions IS
'Business metric definitions and glossary for the Northwind Analytics mart schema.
Each row defines one metric: its business meaning, SQL formula, source table, and an example question.
Use this table to understand what any metric in the mart schema means.
Genie-optimised: ask "what does X mean?" or "explain the churn risk metric" and Genie will consult this table.
Refresh frequency: manual — update when new metrics are added to the mart schema.'
""")

print(f"metric_definitions: {df.count()} metrics defined")

# COMMAND ----------

# MAGIC %md ## 4. Enhanced Column Comments on agg_daily_sales

# COMMAND ----------

agg_col_comments = {
    "order_date":       "Calendar date of the orders in this row. One row per (order_date, store_name, channel) combination. Use this for date range filtering.",
    "year":             "Calendar year (e.g. 2024). Integer. Use YEAR(current_date()) to filter to the current year.",
    "quarter":          "Calendar quarter 1–4 (Q1 = Jan–Mar, Q2 = Apr–Jun, Q3 = Jul–Sep, Q4 = Oct–Dec).",
    "year_quarter":     "Year and quarter as a string label (e.g. '2024-Q3'). Use for grouping and display.",
    "year_month":       "Year and month as 'YYYY-MM' (e.g. '2024-03'). Use for monthly grouping.",
    "month_name":       "Full month name (e.g. 'November'). Use for human-readable labels.",
    "month_num":        "Month number 1–12. Use for sorting monthly data chronologically.",
    "is_weekend":       "TRUE if the order date falls on a Saturday or Sunday. Weekend orders typically have higher AOV.",
    "is_holiday":       "TRUE if the order date is a public holiday (US calendar). Holiday periods have 1.5–2x normal order volume.",
    "fiscal_year":      "Northwind fiscal year, which starts in April. FY2024 = Apr 2024 – Mar 2025.",
    "fiscal_quarter":   "Northwind fiscal quarter label (e.g. 'FY2024-Q1'). Q1 = Apr–Jun, Q2 = Jul–Sep, Q3 = Oct–Dec, Q4 = Jan–Mar.",
    "store_name":       "Name of the store where the order was placed or attributed. 'Online Store' represents all online and mobile app orders.",
    "store_type":       "Store classification: Flagship (large format), Standard, Express (small format), or Online.",
    "store_region":     "Geographic sales region: Northeast, South, Midwest, West (US) or International.",
    "store_country":    "Country of the store. Most stores are United States; International stores are in UK, Canada, Australia.",
    "channel":          "Sales channel through which the order was placed: In-Store, Online, Mobile App, or Phone.",
    "num_orders":       "Count of distinct orders in this row's grouping. Excludes orders with status Processing.",
    "unique_customers": "Count of distinct customers who placed at least one order. A customer visiting multiple stores on the same day is counted once per store.",
    "total_revenue":    "Net revenue (after discounts, before tax). This is the primary revenue metric. Same as net_revenue in v_sales_summary.",
    "gross_revenue":    "Revenue before discounts are applied (sum of subtotals). Gross revenue minus total_discounts equals total_revenue.",
    "total_discounts":  "Sum of all discount amounts. High values indicate promotional activity. As a % of gross_revenue, healthy range is 5–15%.",
    "total_tax":        "Total tax collected across all orders. Tax rates range from 6.25% to 10% depending on store state.",
    "avg_order_value":  "Mean order total (net_revenue / num_orders). Target range: $100–$150. Values below $80 may indicate excessive discounting.",
    "returned_revenue": "Total value of orders that were subsequently returned. Not deducted from total_revenue — use this to calculate net-of-returns revenue.",
    "cancelled_orders": "Count of orders with status Cancelled. Typically 5–10% of daily orders.",
    "promoted_orders":  "Count of orders where a promotional discount was applied. Typically 28% of all orders.",
}

for col, comment in agg_col_comments.items():
    safe_comment = comment.replace("'", "\\'")
    run_sql(
        f"ALTER TABLE {MART}.agg_daily_sales ALTER COLUMN {col} COMMENT '{safe_comment}'",
        ignore_errors=True
    )

print(f"Updated {len(agg_col_comments)} column comments on agg_daily_sales")

# COMMAND ----------

# MAGIC %md ## 5. Enhanced Column Comments on v_kpi_executive and v_sales_current

# COMMAND ----------

kpi_col_comments = {
    "report_date":                          "Date this KPI snapshot was computed. Always equals current_date().",
    "ytd_net_revenue":                      "Year-to-date net revenue from 1 January to today (date-shifted). Primary top-line metric.",
    "ytd_prior_year_revenue":               "Year-to-date net revenue for the same period one year ago. Used to calculate YoY growth.",
    "ytd_growth_pct":                       "Year-over-year revenue growth percentage. Formula: (YTD - Prior YTD) / Prior YTD * 100. Positive = growth.",
    "qtd_net_revenue":                      "Quarter-to-date net revenue from the start of the current calendar quarter.",
    "qtd_prior_year_revenue":               "Quarter-to-date revenue for the same quarter one year ago.",
    "qtd_growth_pct":                       "Quarter-over-quarter (vs same quarter prior year) growth percentage.",
    "mtd_net_revenue":                      "Month-to-date net revenue from the start of the current calendar month.",
    "mtd_avg_order_value":                  "Average order value (AOV) for orders placed this month. Target: $100–$150.",
    "customers_at_churn_risk":              "Number of customers flagged as churn risk (no orders in 60+ days AND fewer than 2 orders in last 90 days).",
    "lifetime_revenue_at_risk":             "Sum of lifetime value for all churn-risk customers. Represents the revenue at stake if these customers do not return.",
    "avg_days_inactive_churn_customers":    "Average number of days since last order for churn-risk customers. Higher values = more severe disengagement.",
    "top_store_by_revenue":                 "Name of the store with the highest total net revenue in the prior full year.",
    "ytd_return_rate_pct":                  "Percentage of YTD orders that were returned. Healthy range: 3–8%. Values above 10% require investigation.",
}

for col, comment in kpi_col_comments.items():
    safe_comment = comment.replace("'", "\\'")
    run_sql(
        f"COMMENT ON COLUMN {MART}.v_kpi_executive.{col} IS '{safe_comment}'",
        ignore_errors=True
    )

print(f"Updated {len(kpi_col_comments)} column comments on v_kpi_executive")

# COMMAND ----------

# MAGIC %md ## 6. Genie Tags on Mart Objects

# COMMAND ----------

mart_tags = {
    "v_sales_summary": {
        "genie_primary_filter_columns": "order_date,year,quarter,store_region,channel,customer_segment",
        "genie_measure_columns":        "net_revenue,gross_revenue,num_orders,avg_order_value,total_discounts",
        "refresh_frequency":            "static_historical",
        "genie_enabled":                "true",
        "recommended_for_genie":        "true",
    },
    "v_sales_current": {
        "genie_primary_filter_columns": "order_date,year,quarter,store_region,channel,customer_segment",
        "genie_measure_columns":        "net_revenue,gross_revenue,num_orders,avg_order_value,total_discounts",
        "refresh_frequency":            "real_time_shifted",
        "genie_enabled":                "true",
        "use_for_current_period":       "true",
        "recommended_for_genie":        "true",
    },
    "v_customer_360": {
        "genie_primary_filter_columns": "customer_segment,loyalty_tier,is_churn_risk,value_tier,region",
        "genie_measure_columns":        "lifetime_value,avg_order_value,days_since_last_order,orders_last_90d",
        "refresh_frequency":            "real_time",
        "genie_enabled":                "true",
        "recommended_for_genie":        "true",
    },
    "v_product_performance": {
        "genie_primary_filter_columns": "category,sub_category,brand,year,month_num",
        "genie_measure_columns":        "revenue,units_sold,gross_margin_pct,return_rate_pct",
        "refresh_frequency":            "real_time",
        "genie_enabled":                "true",
        "recommended_for_genie":        "true",
    },
    "v_store_performance": {
        "genie_primary_filter_columns": "store_name,region,store_type,year,quarter",
        "genie_measure_columns":        "actual_revenue,pct_of_target,revenue_vs_target,revenue_per_sqft",
        "refresh_frequency":            "real_time",
        "genie_enabled":                "true",
        "recommended_for_genie":        "true",
    },
    "v_employee_sales": {
        "genie_primary_filter_columns": "department,store_name,region,year,quarter",
        "genie_measure_columns":        "total_revenue,orders_processed,avg_order_value,discount_rate_pct",
        "refresh_frequency":            "real_time",
        "genie_enabled":                "true",
    },
    "v_cohort_analysis": {
        "genie_primary_filter_columns": "cohort_month,acquisition_channel,period_number",
        "genie_measure_columns":        "retention_rate_pct,cohort_revenue,revenue_per_active_customer",
        "refresh_frequency":            "real_time",
        "genie_enabled":                "true",
    },
    "v_kpi_executive": {
        "genie_primary_filter_columns": "report_date",
        "genie_measure_columns":        "ytd_net_revenue,ytd_growth_pct,customers_at_churn_risk,ytd_return_rate_pct",
        "refresh_frequency":            "real_time",
        "genie_enabled":                "true",
        "use_for_summary_questions":    "true",
        "recommended_for_genie":        "true",
    },
    "agg_daily_sales": {
        "genie_primary_filter_columns": "order_date,store_region,channel,year,quarter",
        "genie_measure_columns":        "total_revenue,num_orders,avg_order_value,unique_customers",
        "refresh_frequency":            "daily_refresh_required",
        "genie_enabled":                "true",
        "recommended_for_genie":        "true",
    },
    "metric_definitions": {
        "genie_enabled":                "true",
        "use_for_metric_lookups":       "true",
        "refresh_frequency":            "manual",
    },
}

for obj_name, tags in mart_tags.items():
    tag_pairs = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
    # Tags on views use ALTER VIEW; tables use ALTER TABLE
    if obj_name in ("agg_daily_sales", "metric_definitions"):
        stmt = f"ALTER TABLE {MART}.{obj_name} SET TAGS ({tag_pairs})"
    else:
        stmt = f"ALTER VIEW {MART}.{obj_name} SET TAGS ({tag_pairs})"
    run_sql(stmt, ignore_errors=True)

print(f"Tags applied to {len(mart_tags)} mart objects")

# COMMAND ----------

# MAGIC %md ## 7. Summary

# COMMAND ----------

print("=" * 60)
print("  Genie Metadata Augmentation — Complete")
print("=" * 60)

summary = [
    ("mart.v_sales_current",    "Date-shifted sales view — enables 'this month' queries"),
    ("mart.v_kpi_executive",    "Single-row executive KPI summary"),
    ("mart.metric_definitions", "Business metric glossary"),
    ("column comments",         "Enhanced on agg_daily_sales and v_kpi_executive"),
    ("tags",                    f"Applied to {len(mart_tags)} mart objects"),
]

for obj, desc in summary:
    print(f"  ✓  {obj:<30} {desc}")

print()
print("Next steps:")
print("  1. Run 09_demo_data_refresh.py to refresh agg_daily_sales and ML timestamps")
print("  2. Create a Genie Space and add: v_sales_current, v_kpi_executive,")
print("     v_customer_360, v_product_performance, v_store_performance,")
print("     agg_daily_sales, v_cohort_analysis, metric_definitions")
print("  3. Add Trusted Asset SQL from IMPROVEMENTS.md Section 3.6")
print("  4. Paste the system instructions from IMPROVEMENTS.md Section 4.2 into")
print("     the Genie Space instructions field")
print("=" * 60)
