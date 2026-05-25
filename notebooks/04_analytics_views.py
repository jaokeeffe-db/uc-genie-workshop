# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Analytics Views & Mart Schema
# MAGIC
# MAGIC Creates the **mart schema** objects optimised for **Genie (AI/BI)** and self-service analytics:
# MAGIC
# MAGIC | Object | Type | Primary Use Case |
# MAGIC |--------|------|-----------------|
# MAGIC | `v_sales_summary` | View | Daily revenue by store, channel, and customer segment |
# MAGIC | `v_customer_360` | View | Customer lifetime value, recency, and churn risk |
# MAGIC | `v_product_performance` | View | Sales, returns, and margin by product/category |
# MAGIC | `v_store_performance` | View | Store revenue vs target, ranking |
# MAGIC | `v_employee_sales` | View | Sales associate performance |
# MAGIC | `v_cohort_analysis` | View | Customer acquisition cohort retention |
# MAGIC | `agg_daily_sales` | Delta Table | Pre-aggregated daily totals — fast trend queries |
# MAGIC
# MAGIC All views include **comprehensive column comments** so Genie understands every field.

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "sample_db", "Catalog Name")
dbutils.widgets.text("env",           "dev",        "Environment")
dbutils.widgets.text("reset_catalog", "false",      "Reset")
dbutils.widgets.text("num_customers", "2000",       "Num Customers")
dbutils.widgets.text("num_orders",    "50000",      "Num Orders")

CATALOG = dbutils.widgets.get("catalog_name")
DIM     = f"{CATALOG}.dimensions"
FACT    = f"{CATALOG}.facts"
MART    = f"{CATALOG}.mart"

print(f"Creating mart objects in: {MART}")

# COMMAND ----------

# MAGIC %md ## v_sales_summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.v_sales_summary
COMMENT 'Daily sales summary pre-joined across orders, dates, stores, and customers.
Primary view for revenue trend analysis, channel performance, and geographic breakdowns.
Best for answering questions like: total revenue last quarter, sales by region, weekend vs weekday performance.
Genie-optimised: all columns have business-friendly names and descriptions.'
AS
SELECT
    d.full_date                                                 AS order_date,
    d.calendar_year                                             AS year,
    d.calendar_quarter                                          AS quarter,
    d.year_quarter,
    d.year_month,
    d.month_name,
    d.month_num,
    d.day_name,
    d.is_weekend,
    d.is_holiday,
    d.fiscal_year,
    d.fiscal_quarter,
    s.store_id,
    s.store_name,
    s.store_type,
    s.city                                                       AS store_city,
    s.state_province                                             AS store_state,
    s.country                                                    AS store_country,
    s.region                                                     AS store_region,
    c.segment                                                    AS customer_segment,
    c.loyalty_tier                                               AS customer_loyalty_tier,
    c.country                                                    AS customer_country,
    o.channel,
    o.payment_method,
    o.shipping_method,
    COUNT(o.order_key)                                           AS num_orders,
    COUNT(DISTINCT o.customer_key)                               AS num_unique_customers,
    SUM(o.subtotal)                                              AS gross_revenue,
    SUM(o.discount_amount)                                       AS total_discounts,
    SUM(o.tax_amount)                                            AS total_tax,
    SUM(o.shipping_amount)                                       AS total_shipping,
    SUM(o.total_amount)                                          AS net_revenue,
    AVG(o.total_amount)                                          AS avg_order_value,
    MIN(o.total_amount)                                          AS min_order_value,
    MAX(o.total_amount)                                          AS max_order_value,
    SUM(CASE WHEN o.is_returned = TRUE THEN 1 ELSE 0 END)        AS num_returned_orders,
    SUM(CASE WHEN o.is_returned = TRUE THEN o.total_amount ELSE 0 END) AS returned_revenue,
    SUM(CASE WHEN o.order_status = 'Cancelled' THEN 1 ELSE 0 END) AS num_cancelled_orders,
    SUM(CASE WHEN o.promotion_key > 0 THEN 1 ELSE 0 END)         AS num_promoted_orders
FROM {FACT}.fact_orders      o
JOIN {DIM}.dim_date          d  ON o.order_date_key = d.date_key
JOIN {DIM}.dim_store         s  ON o.store_key      = s.store_key
JOIN {DIM}.dim_customer      c  ON o.customer_key   = c.customer_key
WHERE o.order_status NOT IN ('Processing')
GROUP BY ALL
""")

print("v_sales_summary created")

# COMMAND ----------

# MAGIC %md ## v_customer_360

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.v_customer_360
COMMENT 'Customer 360-degree view combining demographic profile, purchase history, loyalty status, and churn risk.
Each row represents one customer with aggregated lifetime metrics.
Best for questions like: top customers by lifetime value, customers at churn risk, segment analysis.
Churn risk is flagged when a customer has not ordered in 60+ days and had fewer than 2 orders in the last 90 days.'
AS
WITH order_metrics AS (
    SELECT
        o.customer_key,
        COUNT(DISTINCT o.order_key)                                    AS lifetime_orders,
        SUM(o.total_amount)                                            AS lifetime_value,
        AVG(o.total_amount)                                            AS avg_order_value,
        SUM(o.total_amount) / NULLIF(COUNT(DISTINCT d.year_quarter), 0) AS avg_quarterly_spend,
        MAX(d.full_date)                                               AS last_order_date,
        MIN(d.full_date)                                               AS first_order_date,
        DATEDIFF(current_date(), MAX(d.full_date))                     AS days_since_last_order,
        COUNT(CASE WHEN d.full_date >= date_sub(current_date(), 90) THEN 1 END) AS orders_last_90d,
        SUM(CASE WHEN d.full_date >= date_sub(current_date(), 90) THEN o.total_amount ELSE 0 END) AS spend_last_90d,
        COUNT(DISTINCT o.store_key)                                    AS num_stores_visited,
        MODE(o.channel)                                                AS preferred_channel,
        MODE(o.payment_method)                                         AS preferred_payment,
        SUM(CASE WHEN o.is_returned = TRUE THEN 1 ELSE 0 END)         AS total_returns,
        SUM(CASE WHEN o.is_returned = TRUE THEN o.total_amount ELSE 0 END) AS total_returned_value
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date     d ON o.order_date_key = d.date_key
    GROUP BY o.customer_key
),
item_metrics AS (
    SELECT
        o.customer_key,
        MODE(p.category)  AS preferred_category,
        MODE(p.brand)     AS preferred_brand
    FROM {FACT}.fact_orders      o
    JOIN {FACT}.fact_order_items i  ON o.order_key  = i.order_key
    JOIN {DIM}.dim_product       p  ON i.product_key = p.product_key
    GROUP BY o.customer_key
)
SELECT
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.gender,
    c.date_of_birth,
    c.city,
    c.state_province,
    c.country,
    c.region,
    c.segment                                                           AS customer_segment,
    c.annual_income_band,
    c.loyalty_tier,
    c.loyalty_points,
    c.acquisition_channel,
    c.preferred_payment,
    c.registration_date,
    DATEDIFF(current_date(), c.registration_date)                      AS days_since_registration,
    c.is_active,
    c.marketing_consent,
    COALESCE(m.lifetime_orders, 0)                                     AS lifetime_orders,
    COALESCE(m.lifetime_value, 0.0)                                    AS lifetime_value,
    COALESCE(m.avg_order_value, 0.0)                                   AS avg_order_value,
    COALESCE(m.avg_quarterly_spend, 0.0)                               AS avg_quarterly_spend,
    m.last_order_date,
    m.first_order_date,
    COALESCE(m.days_since_last_order, 9999)                            AS days_since_last_order,
    COALESCE(m.orders_last_90d, 0)                                     AS orders_last_90d,
    COALESCE(m.spend_last_90d, 0.0)                                    AS spend_last_90d,
    COALESCE(m.num_stores_visited, 0)                                  AS num_stores_visited,
    m.preferred_channel,
    m.preferred_payment                                                  AS payment_method_most_used,
    COALESCE(m.total_returns, 0)                                       AS total_returns,
    COALESCE(m.total_returned_value, 0.0)                              AS total_returned_value,
    COALESCE(m.total_returns, 0) / NULLIF(m.lifetime_orders, 0)       AS return_rate,
    im.preferred_category,
    im.preferred_brand,
    (COALESCE(m.days_since_last_order, 9999) > 60
     AND COALESCE(m.orders_last_90d, 0) < 2)                          AS is_churn_risk,
    CASE
        WHEN COALESCE(m.lifetime_value, 0) >= 5000 THEN 'VIP'
        WHEN COALESCE(m.lifetime_value, 0) >= 2000 THEN 'High Value'
        WHEN COALESCE(m.lifetime_value, 0) >= 500  THEN 'Medium Value'
        ELSE 'Low Value'
    END                                                                 AS value_tier
FROM {DIM}.dim_customer      c
LEFT JOIN order_metrics      m  ON c.customer_key = m.customer_key
LEFT JOIN item_metrics       im ON c.customer_key = im.customer_key
""")

print("v_customer_360 created")

# COMMAND ----------

# MAGIC %md ## v_product_performance

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.v_product_performance
COMMENT 'Product performance view aggregating sales, returns, and inventory metrics.
Covers all active products with monthly granularity.
Best for questions like: best-selling products this month, highest return rate products, category revenue breakdown.
Includes gross margin information (requires finance_role for unit_cost visibility).'
AS
SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.sub_category,
    p.sku,
    p.unit_price                                                      AS list_price,
    p.is_active,
    s.year_month,
    s.year,
    s.month_name,
    s.month_num,
    s.quarter,
    COALESCE(s.num_orders, 0)                                         AS num_orders,
    COALESCE(s.units_sold, 0)                                         AS units_sold,
    COALESCE(s.revenue, 0.0)                                          AS revenue,
    COALESCE(s.total_discounts, 0.0)                                  AS total_discounts,
    COALESCE(s.avg_selling_price, p.unit_price)                       AS avg_selling_price,
    COALESCE(s.total_cost, 0.0)                                       AS total_cost,
    COALESCE(s.gross_profit, 0.0)                                     AS gross_profit,
    COALESCE(s.gross_margin_pct, 0.0)                                 AS gross_margin_pct,
    COALESCE(r.return_count, 0)                                       AS return_count,
    COALESCE(r.return_quantity, 0)                                    AS return_quantity,
    COALESCE(r.refund_amount, 0.0)                                    AS refund_amount,
    COALESCE(r.return_quantity, 0) / NULLIF(s.units_sold, 0) * 100   AS return_rate_pct
FROM {DIM}.dim_product p
LEFT JOIN (
    SELECT
        i.product_key,
        d.year_month,
        d.calendar_year                                               AS year,
        d.month_name,
        d.month_num,
        d.calendar_quarter                                            AS quarter,
        COUNT(DISTINCT o.order_key)                                   AS num_orders,
        SUM(i.quantity)                                               AS units_sold,
        SUM(i.line_total)                                             AS revenue,
        SUM(i.discount_amount)                                        AS total_discounts,
        AVG(i.unit_price)                                             AS avg_selling_price,
        SUM(i.unit_cost * i.quantity)                                 AS total_cost,
        SUM(i.line_total) - SUM(i.unit_cost * i.quantity)            AS gross_profit,
        (SUM(i.line_total) - SUM(i.unit_cost * i.quantity))
          / NULLIF(SUM(i.line_total), 0) * 100                       AS gross_margin_pct
    FROM {FACT}.fact_order_items i
    JOIN {FACT}.fact_orders      o ON i.order_key      = o.order_key
    JOIN {DIM}.dim_date          d ON o.order_date_key = d.date_key
    GROUP BY i.product_key, d.year_month, d.calendar_year, d.month_name, d.month_num, d.calendar_quarter
) s ON p.product_key = s.product_key
LEFT JOIN (
    SELECT
        i.product_key,
        d.year_month,
        COUNT(DISTINCT r.return_key)    AS return_count,
        SUM(i.quantity)                 AS return_quantity,
        SUM(r.refund_amount)            AS refund_amount
    FROM {FACT}.fact_returns     r
    JOIN {FACT}.fact_order_items i  ON r.order_key     = i.order_key
    JOIN {DIM}.dim_date          d  ON r.return_date_key = d.date_key
    GROUP BY i.product_key, d.year_month
) r ON p.product_key = r.product_key AND s.year_month = r.year_month
""")

print("v_product_performance created")

# COMMAND ----------

# MAGIC %md ## v_store_performance

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.v_store_performance
COMMENT 'Store performance view comparing actual revenue against annual targets.
Includes month-over-month and year-over-year growth calculations.
Best for questions like: which stores are underperforming, top 5 stores by revenue, store ranking by region.
Annual target is stored in dim_store.annual_target and prorated to monthly for comparison.'
AS
WITH monthly_sales AS (
    SELECT
        o.store_key,
        d.calendar_year                                               AS year,
        d.month_num,
        d.month_name,
        d.year_month,
        d.calendar_quarter                                            AS quarter,
        COUNT(DISTINCT o.order_key)                                   AS num_orders,
        COUNT(DISTINCT o.customer_key)                                AS unique_customers,
        SUM(o.total_amount)                                           AS actual_revenue,
        AVG(o.total_amount)                                           AS avg_order_value,
        SUM(o.discount_amount)                                        AS total_discounts,
        SUM(CASE WHEN o.is_returned = TRUE THEN 1 ELSE 0 END)        AS returns_count
    FROM {FACT}.fact_orders  o
    JOIN {DIM}.dim_date      d ON o.order_date_key = d.date_key
    WHERE o.order_status NOT IN ('Processing')
    GROUP BY o.store_key, d.calendar_year, d.month_num, d.month_name, d.year_month, d.calendar_quarter
)
SELECT
    s.store_key,
    s.store_id,
    s.store_name,
    s.store_type,
    s.city,
    s.state_province,
    s.country,
    s.region,
    s.floor_area_sqft,
    s.num_employees,
    s.annual_target,
    s.annual_target / 12.0                                            AS monthly_target,
    m.year,
    m.month_num,
    m.month_name,
    m.year_month,
    m.quarter,
    COALESCE(m.num_orders, 0)                                         AS num_orders,
    COALESCE(m.unique_customers, 0)                                   AS unique_customers,
    COALESCE(m.actual_revenue, 0.0)                                   AS actual_revenue,
    COALESCE(m.avg_order_value, 0.0)                                  AS avg_order_value,
    COALESCE(m.total_discounts, 0.0)                                  AS total_discounts,
    COALESCE(m.returns_count, 0)                                      AS returns_count,
    COALESCE(m.actual_revenue, 0.0) - (s.annual_target / 12.0)       AS revenue_vs_target,
    COALESCE(m.actual_revenue, 0.0) / NULLIF((s.annual_target / 12.0), 0) * 100 AS pct_of_target,
    COALESCE(m.actual_revenue, 0.0) / NULLIF(s.num_employees, 0)     AS revenue_per_employee,
    CASE WHEN s.floor_area_sqft > 0
         THEN COALESCE(m.actual_revenue, 0.0) / s.floor_area_sqft
         ELSE NULL END                                                AS revenue_per_sqft
FROM {DIM}.dim_store          s
CROSS JOIN (SELECT DISTINCT d.calendar_year AS year, d.month_num, d.month_name, d.year_month, d.calendar_quarter AS quarter FROM {FACT}.fact_orders o JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key) dates
LEFT JOIN monthly_sales       m ON s.store_key = m.store_key AND dates.year = m.year AND dates.month_num = m.month_num
WHERE s.is_active = TRUE
""")

print("v_store_performance created")

# COMMAND ----------

# MAGIC %md ## v_employee_sales

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.v_employee_sales
COMMENT 'Employee sales performance view showing orders processed and revenue attributed per associate.
Best for questions like: top performing sales associates, which employees need coaching, quarterly rankings.
Note: channel Online and Mobile App orders are attributed to the store employee as the processing agent.'
AS
SELECT
    e.employee_key,
    e.employee_id,
    e.first_name,
    e.last_name,
    CONCAT(e.first_name, ' ', e.last_name)                            AS full_name,
    e.department,
    e.job_title,
    e.performance_rating,
    st.store_name,
    st.region,
    d.calendar_year                                                    AS year,
    d.calendar_quarter                                                 AS quarter,
    d.month_name,
    d.month_num,
    d.year_month,
    COUNT(DISTINCT o.order_key)                                        AS orders_processed,
    COUNT(DISTINCT o.customer_key)                                     AS unique_customers_served,
    SUM(o.total_amount)                                                AS total_revenue,
    AVG(o.total_amount)                                                AS avg_order_value,
    SUM(o.discount_amount)                                             AS total_discounts_given,
    SUM(o.discount_amount) / NULLIF(SUM(o.subtotal), 0) * 100        AS discount_rate_pct
FROM {DIM}.dim_employee      e
JOIN {DIM}.dim_store         st ON e.store_key     = st.store_key
JOIN {FACT}.fact_orders      o  ON e.employee_key  = o.employee_key
JOIN {DIM}.dim_date          d  ON o.order_date_key = d.date_key
WHERE e.is_active = TRUE
  AND o.order_status NOT IN ('Processing', 'Cancelled')
GROUP BY ALL
""")

print("v_employee_sales created")

# COMMAND ----------

# MAGIC %md ## v_cohort_analysis

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.v_cohort_analysis
COMMENT 'Customer acquisition cohort analysis showing retention over monthly periods.
Each row represents a cohort (customers who first ordered in a given month) and their activity in subsequent months.
Best for questions like: what is the 3-month retention rate for 2023 cohorts, how do different acquisition channels retain customers.
cohort_month = month of first purchase. period_number = months since first purchase (0 = acquisition month).'
AS
WITH first_orders AS (
    SELECT
        o.customer_key,
        MIN(d.full_date)                                              AS first_order_date,
        date_format(MIN(d.full_date), 'yyyy-MM')                      AS cohort_month
    FROM {FACT}.fact_orders  o
    JOIN {DIM}.dim_date      d ON o.order_date_key = d.date_key
    GROUP BY o.customer_key
),
cohort_activity AS (
    SELECT
        fo.cohort_month,
        date_format(d.full_date, 'yyyy-MM')                           AS activity_month,
        MONTHS_BETWEEN(
            date_trunc('month', d.full_date),
            date_trunc('month', fo.first_order_date)
        )                                                              AS period_number,
        COUNT(DISTINCT o.customer_key)                                AS active_customers,
        SUM(o.total_amount)                                           AS cohort_revenue
    FROM first_orders        fo
    JOIN {FACT}.fact_orders  o  ON fo.customer_key = o.customer_key
    JOIN {DIM}.dim_date      d  ON o.order_date_key = d.date_key
    GROUP BY fo.cohort_month, date_format(d.full_date, 'yyyy-MM'),
             MONTHS_BETWEEN(date_trunc('month', d.full_date), date_trunc('month', fo.first_order_date))
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT customer_key) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_size,
    c.acquisition_channel,
    ca.activity_month,
    CAST(ca.period_number AS INT)                                     AS period_number,
    ca.active_customers,
    ca.cohort_revenue,
    ca.active_customers / cs.cohort_size * 100                        AS retention_rate_pct,
    ca.cohort_revenue / ca.active_customers                           AS revenue_per_active_customer
FROM cohort_activity    ca
JOIN cohort_sizes       cs ON ca.cohort_month = cs.cohort_month
JOIN first_orders       fo ON ca.cohort_month = fo.cohort_month
JOIN {DIM}.dim_customer c  ON fo.customer_key = c.customer_key
WHERE ca.period_number >= 0 AND ca.period_number <= 24
GROUP BY ALL
""")

print("v_cohort_analysis created")

# COMMAND ----------

# MAGIC %md ## agg_daily_sales (materialised aggregate table)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {MART}.agg_daily_sales
COMMENT 'Pre-aggregated daily sales totals by store and channel.
This is a physical Delta table (not a view) for fast Genie and BI queries on revenue trends.
Liquid-clustered on (order_date, store_region) for optimal date-range and regional filter performance.
Refresh this table daily by running: INSERT OVERWRITE {MART}.agg_daily_sales SELECT ... (re-run this notebook).'
AS
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

spark.sql(f"ALTER TABLE {MART}.agg_daily_sales CLUSTER BY (order_date, store_region)")
spark.sql(f"""ALTER TABLE {MART}.agg_daily_sales SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'pipelines.reset.allowed'     = 'true'
)""")

count = spark.table(f"{MART}.agg_daily_sales").count()
print(f"agg_daily_sales: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("Mart schema objects:")
objects = spark.sql(f"SHOW TABLES IN {CATALOG}.mart").collect()
for obj in objects:
    cnt = spark.table(f"{MART}.{obj['tableName']}").count()
    print(f"  {obj['tableName']:<35} {cnt:>10,} rows")

print(f"\n{len(objects)} objects created in {MART}")
print("""
Suggested Genie Space tables:
  • v_sales_summary        — revenue trends, channel, geography
  • v_customer_360         — customer LTV, churn risk, segments
  • v_product_performance  — product sales, returns, margin
  • v_store_performance    — store rankings, vs target
  • agg_daily_sales        — fast daily/weekly/monthly aggregates
""")
