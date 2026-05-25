# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 11 — Metric Views (Genie Semantic Layer)
# MAGIC
# MAGIC Creates **metric views** in the `mart` schema — a semantic layer on top of the Northwind
# MAGIC Analytics data model that Genie (AI/BI) uses to answer natural language questions with
# MAGIC pre-defined, trustworthy KPI definitions.
# MAGIC
# MAGIC | Metric View | Source | Business Domain |
# MAGIC |-------------|--------|-----------------|
# MAGIC | `mv_sales` | `mart.agg_daily_sales` | Daily revenue, orders, discount trends |
# MAGIC | `mv_customers` | `mart.v_customer_360` | Customer health, LTV, churn risk |
# MAGIC | `mv_products` | `mart.v_product_performance` | Product sales, profitability, returns |
# MAGIC | `mv_stores` | `mart.v_store_performance` | Store revenue vs target, efficiency |
# MAGIC
# MAGIC Each metric view defines:
# MAGIC - **Dimensions** — categorical attributes for grouping and filtering
# MAGIC - **Measures** — pre-approved aggregate KPIs with business-friendly names and synonyms
# MAGIC
# MAGIC Metric views require **DBR 15.4 LTS+** or a Serverless SQL warehouse.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "sample_db", "Catalog Name")

CATALOG = dbutils.widgets.get("catalog_name")
MART    = f"{CATALOG}.mart"

print(f"Creating metric views in: {MART}")

# COMMAND ----------

# MAGIC %md ## mv_sales — Daily Sales & Revenue KPIs

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.mv_sales
WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: >
  Daily sales and revenue KPIs for the Northwind retail business.
  Source is the pre-aggregated daily sales table (agg_daily_sales) for fast query performance.
  Use this metric view to answer questions about revenue trends, order volumes, discount rates,
  channel mix, and regional performance over time.
source: {MART}.agg_daily_sales
dimensions:
  - name: order_date
    expr: order_date
    display_name: Order Date
    comment: The calendar date on which orders were placed.
    synonyms:
      - date
      - sale date
      - transaction date
      - purchase date
  - name: year
    expr: year
    display_name: Year
    comment: Calendar year of the order date.
    synonyms:
      - calendar year
      - sales year
  - name: quarter
    expr: quarter
    display_name: Quarter
    comment: Calendar quarter (1–4) of the order date.
    synonyms:
      - fiscal quarter
      - Q1
      - Q2
      - Q3
      - Q4
  - name: month_name
    expr: month_name
    display_name: Month
    comment: Full month name (e.g. January, February) of the order date.
    synonyms:
      - month
      - calendar month
  - name: year_month
    expr: year_month
    display_name: Year-Month
    comment: Year and month in YYYY-MM format for time-series grouping (e.g. 2024-03).
    synonyms:
      - monthly period
      - month period
  - name: is_weekend
    expr: is_weekend
    display_name: Is Weekend
    comment: Whether orders were placed on a Saturday or Sunday (true/false).
    synonyms:
      - weekend flag
      - weekend vs weekday
  - name: is_holiday
    expr: is_holiday
    display_name: Is Holiday
    comment: Whether the order date falls on a public holiday (true/false).
    synonyms:
      - holiday flag
      - public holiday
  - name: store_name
    expr: store_name
    display_name: Store Name
    comment: Name of the store that processed the orders.
    synonyms:
      - shop
      - location name
      - outlet
  - name: store_type
    expr: store_type
    display_name: Store Type
    comment: Type of store — Flagship, Standard, Outlet, or Online.
    synonyms:
      - channel type
      - store format
  - name: store_region
    expr: store_region
    display_name: Store Region
    comment: Geographic region of the store (e.g. North America, Europe, Asia Pacific).
    synonyms:
      - region
      - geographic region
      - sales region
  - name: store_country
    expr: store_country
    display_name: Store Country
    comment: Country where the store is located.
    synonyms:
      - country
      - market
  - name: channel
    expr: channel
    display_name: Sales Channel
    comment: Order channel — In-Store, Online, or Mobile App.
    synonyms:
      - order channel
      - fulfilment channel
      - online vs in-store
measures:
  - name: total_revenue
    expr: SUM(total_revenue)
    display_name: Total Revenue
    comment: >
      Net revenue after discounts, inclusive of tax and shipping.
      This is the primary top-line revenue metric.
    synonyms:
      - revenue
      - net revenue
      - sales
      - total sales
      - income
  - name: gross_revenue
    expr: SUM(gross_revenue)
    display_name: Gross Revenue
    comment: Revenue before discounts are applied (subtotal only, excluding tax and shipping).
    synonyms:
      - pre-discount revenue
      - subtotal
      - gross sales
  - name: total_discounts
    expr: SUM(total_discounts)
    display_name: Total Discounts
    comment: Total value of discounts applied across all orders.
    synonyms:
      - discounts
      - discount amount
      - promotions applied
  - name: discount_rate
    expr: MEASURE(total_discounts) / MEASURE(gross_revenue) * 100
    display_name: Discount Rate (%)
    comment: Percentage of gross revenue given away as discounts. Lower is better for margin.
    synonyms:
      - discount percentage
      - promotion rate
      - markdown rate
  - name: order_count
    expr: SUM(num_orders)
    display_name: Order Count
    comment: Total number of completed orders (excludes Processing status).
    synonyms:
      - orders
      - number of orders
      - transaction count
      - number of transactions
  - name: unique_customers
    expr: SUM(unique_customers)
    display_name: Unique Customers
    comment: >
      Count of distinct customers per day and store. Note: summing across days
      may count repeat shoppers more than once — use for daily trend analysis.
    synonyms:
      - customer count
      - customers served
      - buyers
  - name: avg_order_value
    expr: MEASURE(total_revenue) / MEASURE(order_count)
    display_name: Average Order Value
    comment: Average net revenue per order. Key indicator of basket size and upsell effectiveness.
    synonyms:
      - AOV
      - basket size
      - average basket
      - average transaction value
  - name: returned_revenue
    expr: SUM(returned_revenue)
    display_name: Returned Revenue
    comment: Revenue from orders that were subsequently returned.
    synonyms:
      - returns value
      - refund amount
      - return revenue
  - name: return_rate
    expr: MEASURE(returned_revenue) / MEASURE(total_revenue) * 100
    display_name: Return Rate (%)
    comment: Percentage of revenue that was returned. High values indicate fulfilment or quality issues.
    synonyms:
      - refund rate
      - return percentage
  - name: cancelled_orders
    expr: SUM(cancelled_orders)
    display_name: Cancelled Orders
    comment: Number of orders that were cancelled before fulfilment.
    synonyms:
      - cancellations
      - order cancellations
  - name: promoted_orders
    expr: SUM(promoted_orders)
    display_name: Promoted Orders
    comment: Number of orders that included at least one promotional discount.
    synonyms:
      - promotional orders
      - orders with promotions
      - discounted orders
$$
""")

print("mv_sales created")

# COMMAND ----------

# MAGIC %md ## mv_customers — Customer Health & Lifetime Value KPIs

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.mv_customers
WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: >
  Customer health, lifetime value, and churn risk KPIs.
  Source is the v_customer_360 view — one row per customer.
  Use this metric view to answer questions about customer segmentation, LTV distribution,
  churn risk cohorts, and acquisition channel effectiveness.
source: {MART}.v_customer_360
dimensions:
  - name: customer_segment
    expr: customer_segment
    display_name: Customer Segment
    comment: Business segment — Consumer, Corporate, Home Office, or Small Business.
    synonyms:
      - segment
      - business segment
      - customer type
  - name: loyalty_tier
    expr: loyalty_tier
    display_name: Loyalty Tier
    comment: Loyalty programme tier — Bronze, Silver, Gold, or Platinum.
    synonyms:
      - loyalty level
      - tier
      - membership tier
  - name: value_tier
    expr: value_tier
    display_name: Value Tier
    comment: >
      Customer value classification based on lifetime spend:
      VIP (>=5000), High Value (>=2000), Medium Value (>=500), Low Value (<500).
    synonyms:
      - LTV tier
      - spend tier
      - customer value category
  - name: region
    expr: region
    display_name: Region
    comment: Geographic region of the customer's home address.
    synonyms:
      - customer region
      - geographic region
  - name: country
    expr: country
    display_name: Country
    comment: Country of the customer's home address.
    synonyms:
      - customer country
      - market
  - name: gender
    expr: gender
    display_name: Gender
    comment: Customer gender (Male, Female, or Non-binary).
    synonyms:
      - sex
  - name: annual_income_band
    expr: annual_income_band
    display_name: Income Band
    comment: Annual household income bracket (<30k, 30k-60k, 60k-100k, 100k-150k, 150k+).
    synonyms:
      - income bracket
      - income level
      - household income
  - name: acquisition_channel
    expr: acquisition_channel
    display_name: Acquisition Channel
    comment: The channel through which the customer first registered (e.g. Online, In-Store, Referral).
    synonyms:
      - how customer acquired
      - signup channel
      - registration channel
  - name: preferred_channel
    expr: preferred_channel
    display_name: Preferred Channel
    comment: The channel the customer uses most often for purchases.
    synonyms:
      - most used channel
      - favourite channel
  - name: is_active
    expr: is_active
    display_name: Is Active
    comment: Whether the customer account is currently active (true/false).
    synonyms:
      - active flag
      - active customers
  - name: is_churn_risk
    expr: is_churn_risk
    display_name: Is Churn Risk
    comment: >
      Churn risk flag (true/false). A customer is at churn risk if they have not ordered
      in 60+ days AND placed fewer than 2 orders in the last 90 days.
    synonyms:
      - churn flag
      - at risk
      - likely to churn
measures:
  - name: customer_count
    expr: COUNT(1)
    display_name: Customer Count
    comment: Total number of customers (active and inactive).
    synonyms:
      - customers
      - number of customers
      - total customers
  - name: active_customers
    expr: COUNT(CASE WHEN is_active THEN 1 END)
    display_name: Active Customers
    comment: Number of customers with an active account status.
    synonyms:
      - active customer count
      - live customers
  - name: churn_risk_customers
    expr: COUNT(CASE WHEN is_churn_risk THEN 1 END)
    display_name: Churn Risk Customers
    comment: Number of customers flagged as at risk of churning.
    synonyms:
      - at-risk customers
      - customers at risk
      - likely churners
  - name: churn_risk_rate
    expr: COUNT(CASE WHEN is_churn_risk THEN 1 END) / COUNT(CASE WHEN is_active THEN 1 END) * 100
    display_name: Churn Risk Rate (%)
    comment: Percentage of active customers currently flagged as churn risk.
    synonyms:
      - churn rate
      - at-risk percentage
      - churn risk percentage
  - name: total_lifetime_value
    expr: SUM(lifetime_value)
    display_name: Total Lifetime Value
    comment: Sum of all revenue generated across all orders for the selected customers.
    synonyms:
      - total LTV
      - total revenue from customers
      - cumulative spend
  - name: avg_lifetime_value
    expr: AVG(lifetime_value)
    display_name: Avg Lifetime Value
    comment: Average revenue per customer across their entire purchase history.
    synonyms:
      - average LTV
      - average customer value
      - LTV
      - average spend per customer
  - name: avg_orders_per_customer
    expr: AVG(lifetime_orders)
    display_name: Avg Orders per Customer
    comment: Average number of orders placed per customer over their lifetime.
    synonyms:
      - average order frequency
      - purchase frequency
      - orders per customer
  - name: avg_days_since_last_order
    expr: AVG(CASE WHEN days_since_last_order < 9999 THEN days_since_last_order END)
    display_name: Avg Days Since Last Order
    comment: >
      Average number of days since each customer's most recent order.
      Customers who have never ordered are excluded.
    synonyms:
      - recency
      - average recency
      - days inactive
  - name: total_loyalty_points
    expr: SUM(loyalty_points)
    display_name: Total Loyalty Points
    comment: Total loyalty points outstanding across all customers.
    synonyms:
      - loyalty points
      - points balance
  - name: total_returns
    expr: SUM(total_returns)
    display_name: Total Returns
    comment: Total number of return transactions across all customers.
    synonyms:
      - customer returns
      - returns
  - name: avg_return_rate
    expr: AVG(return_rate)
    display_name: Avg Return Rate
    comment: Average return rate per customer (returns / lifetime orders).
    synonyms:
      - return rate
      - refund rate per customer
$$
""")

print("mv_customers created")

# COMMAND ----------

# MAGIC %md ## mv_products — Product Sales & Profitability KPIs

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.mv_products
WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: >
  Product sales, profitability, and returns KPIs at monthly granularity.
  Source is the v_product_performance view — one row per product per month.
  Use this metric view to answer questions about best-selling products, category margins,
  return rates, and discount impact on gross profit.
source: {MART}.v_product_performance
dimensions:
  - name: category
    expr: category
    display_name: Category
    comment: Top-level product category (e.g. Electronics, Clothing, Food & Beverage).
    synonyms:
      - product category
      - department
  - name: sub_category
    expr: sub_category
    display_name: Sub-Category
    comment: Product sub-category within the parent category.
    synonyms:
      - subcategory
      - product sub-category
      - product type
  - name: brand
    expr: brand
    display_name: Brand
    comment: Product brand name.
    synonyms:
      - manufacturer
      - label
  - name: product_name
    expr: product_name
    display_name: Product Name
    comment: Full product name as listed in the catalogue.
    synonyms:
      - product
      - item name
      - SKU name
  - name: year_month
    expr: year_month
    display_name: Year-Month
    comment: Year and month in YYYY-MM format (e.g. 2024-03).
    synonyms:
      - month
      - monthly period
  - name: year
    expr: year
    display_name: Year
    comment: Calendar year of the sales period.
    synonyms:
      - sales year
      - calendar year
  - name: quarter
    expr: quarter
    display_name: Quarter
    comment: Calendar quarter (1–4) of the sales period.
    synonyms:
      - Q1
      - Q2
      - Q3
      - Q4
  - name: month_name
    expr: month_name
    display_name: Month
    comment: Full month name (e.g. January).
    synonyms:
      - calendar month
  - name: is_active
    expr: is_active
    display_name: Is Active
    comment: Whether the product is currently active in the catalogue.
    synonyms:
      - active product
      - in catalogue
      - available
measures:
  - name: total_revenue
    expr: SUM(revenue)
    display_name: Product Revenue
    comment: Total net revenue generated by the product (after discounts).
    synonyms:
      - revenue
      - sales revenue
      - product sales
  - name: total_units_sold
    expr: SUM(units_sold)
    display_name: Units Sold
    comment: Total quantity of units sold.
    synonyms:
      - quantity sold
      - volume sold
      - units
  - name: total_gross_profit
    expr: SUM(gross_profit)
    display_name: Gross Profit
    comment: Revenue minus cost of goods sold (COGS). Requires finance_role for full visibility.
    synonyms:
      - profit
      - margin amount
      - contribution margin
  - name: gross_margin_pct
    expr: SUM(gross_profit) / SUM(revenue) * 100
    display_name: Gross Margin (%)
    comment: Gross profit as a percentage of revenue. Higher is better.
    synonyms:
      - margin
      - margin percentage
      - gross margin
      - profitability
  - name: total_cost
    expr: SUM(total_cost)
    display_name: Total COGS
    comment: Total cost of goods sold. Requires finance_role for visibility.
    synonyms:
      - cost
      - COGS
      - cost of sales
  - name: total_discounts
    expr: SUM(total_discounts)
    display_name: Total Discounts
    comment: Total discount value applied to sales of this product.
    synonyms:
      - discounts
      - markdown value
      - promotion discount
  - name: avg_selling_price
    expr: SUM(revenue) / SUM(units_sold)
    display_name: Avg Selling Price
    comment: Average price at which the product was sold after discounts.
    synonyms:
      - ASP
      - average price
      - average unit price
  - name: return_count
    expr: SUM(return_count)
    display_name: Return Count
    comment: Number of return transactions for the product.
    synonyms:
      - returns
      - product returns
  - name: refund_amount
    expr: SUM(refund_amount)
    display_name: Refund Amount
    comment: Total value of refunds issued for returns of this product.
    synonyms:
      - refunds
      - returned value
  - name: avg_return_rate_pct
    expr: MEASURE(return_count) / MEASURE(total_units_sold) * 100
    display_name: Return Rate (%)
    comment: Percentage of units sold that were subsequently returned.
    synonyms:
      - return rate
      - refund rate
      - return percentage
$$
""")

print("mv_products created")

# COMMAND ----------

# MAGIC %md ## mv_stores — Store Performance vs Target KPIs

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {MART}.mv_stores
WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: >
  Store revenue performance vs monthly targets, and operational efficiency KPIs.
  Source is the v_store_performance view — one row per store per month.
  Use this metric view to answer questions about underperforming stores, regional rankings,
  target attainment, and revenue efficiency per employee or per square foot.
source: {MART}.v_store_performance
dimensions:
  - name: store_name
    expr: store_name
    display_name: Store Name
    comment: Name of the store.
    synonyms:
      - store
      - shop
      - location
      - outlet
  - name: store_type
    expr: store_type
    display_name: Store Type
    comment: Store format — Flagship, Standard, Outlet, or Online.
    synonyms:
      - store format
      - outlet type
  - name: city
    expr: city
    display_name: City
    comment: City where the store is located.
    synonyms:
      - store city
      - location city
  - name: state_province
    expr: state_province
    display_name: State / Province
    comment: State or province where the store is located.
    synonyms:
      - state
      - province
  - name: country
    expr: country
    display_name: Country
    comment: Country where the store is located.
    synonyms:
      - store country
      - market
  - name: region
    expr: region
    display_name: Region
    comment: Geographic sales region (e.g. North America, Europe, Asia Pacific).
    synonyms:
      - store region
      - sales region
      - geographic region
  - name: year
    expr: year
    display_name: Year
    comment: Calendar year of the performance period.
    synonyms:
      - sales year
      - calendar year
  - name: quarter
    expr: quarter
    display_name: Quarter
    comment: Calendar quarter (1–4) of the performance period.
    synonyms:
      - Q1
      - Q2
      - Q3
      - Q4
  - name: month_name
    expr: month_name
    display_name: Month
    comment: Full month name of the performance period.
    synonyms:
      - calendar month
  - name: year_month
    expr: year_month
    display_name: Year-Month
    comment: Year and month in YYYY-MM format (e.g. 2024-03).
    synonyms:
      - monthly period
measures:
  - name: total_revenue
    expr: SUM(actual_revenue)
    display_name: Total Revenue
    comment: Total net revenue generated by the store in the period.
    synonyms:
      - revenue
      - store revenue
      - sales
      - actual revenue
  - name: total_orders
    expr: SUM(num_orders)
    display_name: Order Count
    comment: Total number of orders processed by the store.
    synonyms:
      - orders
      - number of orders
      - transactions
  - name: unique_customers
    expr: SUM(unique_customers)
    display_name: Unique Customers
    comment: Total unique customers served per store per month.
    synonyms:
      - customer count
      - customers served
  - name: avg_order_value
    expr: SUM(actual_revenue) / SUM(num_orders)
    display_name: Avg Order Value
    comment: Average net revenue per order for the store.
    synonyms:
      - AOV
      - basket size
      - average transaction value
  - name: monthly_target
    expr: SUM(monthly_target)
    display_name: Monthly Revenue Target
    comment: >
      Prorated monthly revenue target derived from the store's annual target
      (annual_target / 12). Summing across stores gives portfolio target.
    synonyms:
      - target
      - revenue target
      - sales target
  - name: revenue_vs_target
    expr: SUM(revenue_vs_target)
    display_name: Revenue vs Target
    comment: >
      Actual revenue minus monthly target. Positive = above target, negative = below target.
    synonyms:
      - vs target
      - target variance
      - over/under target
      - performance vs target
  - name: pct_of_target
    expr: MEASURE(total_revenue) / MEASURE(monthly_target) * 100
    display_name: "% of Target Achieved"
    comment: Actual revenue as a percentage of the monthly target. 100% = on target.
    synonyms:
      - target attainment
      - target achievement
      - target percentage
      - percent of target
  - name: total_discounts
    expr: SUM(total_discounts)
    display_name: Total Discounts
    comment: Total discount value applied to orders processed by the store.
    synonyms:
      - discounts
      - promotions
  - name: returns_count
    expr: SUM(returns_count)
    display_name: Returns Count
    comment: Number of returned orders processed at the store.
    synonyms:
      - returns
      - refunds
  - name: avg_revenue_per_employee
    expr: SUM(actual_revenue) / SUM(num_employees)
    display_name: Revenue per Employee
    comment: Revenue divided by headcount — a measure of staff productivity.
    synonyms:
      - revenue per head
      - staff productivity
      - sales per employee
  - name: avg_revenue_per_sqft
    expr: SUM(actual_revenue) / AVG(floor_area_sqft)
    display_name: Revenue per Sq Ft
    comment: Revenue divided by store floor area. A retail efficiency metric.
    synonyms:
      - sales per square foot
      - revenue density
      - space productivity
$$
""")

print("mv_stores created")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

metric_views = ["mv_sales", "mv_customers", "mv_products", "mv_stores"]

print(f"Metric views created in {MART}:")
for mv in metric_views:
    try:
        spark.sql(f"DESCRIBE {MART}.{mv}")
        print(f"  ✓  {mv}")
    except Exception as e:
        print(f"  ✗  {mv} — {e}")

print("""
Suggested Genie Space setup:
  Add all four metric views to a single Genie Space for cross-domain queries.

  Example questions Genie can now answer:
  • "What was total revenue last quarter by region?"
  • "Which stores are below target this month?"
  • "Show me the top 5 products by gross margin in 2024"
  • "How many customers are at churn risk by loyalty tier?"
  • "What is the average order value trend month over month?"
  • "Which acquisition channel has the highest average lifetime value?"
  • "What is the return rate for Electronics vs Clothing?"
  • "Compare revenue per square foot across store types"
""")
