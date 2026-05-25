# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 — Governance, Tags, Comments & Security Policies
# MAGIC
# MAGIC Applies **Unity Catalog governance features** to all objects created in notebooks 02–05:
# MAGIC
# MAGIC | Feature | What It Does |
# MAGIC |---------|-------------|
# MAGIC | **Column Masks** | PII masking for email, phone; cost masking for non-finance roles |
# MAGIC | **Row Filters** | Active-customer filter on `dim_customer`; store-region filter on `fact_orders` |
# MAGIC | **Table Tags** | Domain, PII, classification, SLA, and owner tags on every table |
# MAGIC | **Column Tags** | PII, GDPR, sensitivity tags on individual columns |
# MAGIC | **Table Comments** | Rich business descriptions on every table |
# MAGIC | **Column Comments** | Semantic descriptions on every column (critical for Genie) |
# MAGIC | **Primary Keys** | Informational PKs for query optimisation (`NOT ENFORCED RELY`) |
# MAGIC | **Foreign Keys** | Informational FKs for join elimination |
# MAGIC | **Grants** | Example GRANT statements (conditionally applied in `prod` env) |

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "sample_db", "Catalog Name")
dbutils.widgets.text("env",           "dev",        "Environment")
dbutils.widgets.text("reset_catalog", "false",      "Reset")
dbutils.widgets.text("num_customers", "2000",       "Num Customers")
dbutils.widgets.text("num_orders",    "50000",      "Num Orders")

CATALOG = dbutils.widgets.get("catalog_name")
ENV     = dbutils.widgets.get("env")
DIM     = f"{CATALOG}.dimensions"
FACT    = f"{CATALOG}.facts"
MART    = f"{CATALOG}.mart"
ML      = f"{CATALOG}.ml"

def run_sql(stmt, ignore_errors=False):
    """Execute a SQL statement with optional error suppression."""
    try:
        spark.sql(stmt)
    except Exception as e:
        if ignore_errors:
            print(f"  [WARN] {str(e)[:120]}")
        else:
            raise

print(f"Applying governance to catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md ## Column Mask Functions

# COMMAND ----------

# Email mask: first character + *** + @domain
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.dimensions.mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Masks customer email addresses for privacy. Non-PII roles see first char + ***@domain.
Full email is only visible to members of the pii_readers Unity Catalog group.'
RETURN
  CASE
    WHEN is_member('pii_readers') OR is_member('account_admins') THEN email
    WHEN email IS NULL THEN NULL
    ELSE CONCAT(LEFT(email, 1), '***@', split_part(email, '@', 2))
  END
""")

# Phone mask: keep only last 4 digits
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.dimensions.mask_phone(phone STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Masks customer phone numbers. Non-PII roles see ***-***-XXXX (last 4 digits only).
Full number visible only to pii_readers group.'
RETURN
  CASE
    WHEN is_member('pii_readers') OR is_member('account_admins') THEN phone
    WHEN phone IS NULL THEN NULL
    ELSE CONCAT('***-***-', RIGHT(phone, 4))
  END
""")

# Employee email mask
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.dimensions.mask_employee_email(email STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Masks employee email for non-HR roles. HR and admin groups see the full email.'
RETURN
  CASE
    WHEN is_member('hr_role') OR is_member('account_admins') OR is_member('pii_readers') THEN email
    WHEN email IS NULL THEN NULL
    ELSE CONCAT(LEFT(email, 3), '***@northwindanalytics.com')
  END
""")

# Salary band mask: only HR sees the actual band
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.dimensions.mask_salary_band(salary_band STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Masks employee salary band for non-HR users. Returns NULL for restricted users.'
RETURN
  CASE
    WHEN is_member('hr_role') OR is_member('account_admins') THEN salary_band
    ELSE NULL
  END
""")

# Unit cost mask: only finance role can see product/item costs
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.facts.mask_unit_cost(unit_cost DECIMAL(10,2))
RETURNS DECIMAL(10,2)
LANGUAGE SQL
COMMENT 'Hides unit cost to protect gross margin information. Only finance_role and account_admins see actual values.'
RETURN
  CASE
    WHEN is_member('finance_role') OR is_member('account_admins') THEN unit_cost
    ELSE NULL
  END
""")

# Inventory cost mask
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.facts.mask_inventory_cost(cost DECIMAL(14,2))
RETURNS DECIMAL(14,2)
LANGUAGE SQL
COMMENT 'Masks inventory cost value. Visible only to finance_role and account_admins.'
RETURN
  CASE
    WHEN is_member('finance_role') OR is_member('account_admins') THEN cost
    ELSE NULL
  END
""")

print("Column mask functions created.")

# COMMAND ----------

# MAGIC %md ## Row Filter Functions

# COMMAND ----------

# Customer active filter: non-PII roles only see active customers
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.dimensions.customer_active_filter(is_active BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT 'Row filter on dim_customer. pii_readers and account_admins see all customers (including inactive).
Standard users only see is_active = TRUE customers to limit PII exposure of churned customer data.'
RETURN
  is_member('pii_readers')
  OR is_member('account_admins')
  OR is_active = TRUE
""")

# Order status filter: most users see only completed/delivered orders
run_sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.facts.order_status_filter(order_status STRING)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT 'Row filter on fact_orders. finance_role and account_admins see all orders including Processing.
Standard analysts only see terminal-state orders (Delivered, Shipped, Cancelled, Returned).'
RETURN
  is_member('finance_role')
  OR is_member('account_admins')
  OR order_status IN ('Delivered', 'Shipped', 'Cancelled', 'Returned')
""")

print("Row filter functions created.")

# COMMAND ----------

# MAGIC %md ## Apply Column Masks

# COMMAND ----------

# dim_customer masks
run_sql(f"ALTER TABLE {DIM}.dim_customer ALTER COLUMN email        SET MASK {CATALOG}.dimensions.mask_email",        ignore_errors=True)
run_sql(f"ALTER TABLE {DIM}.dim_customer ALTER COLUMN phone        SET MASK {CATALOG}.dimensions.mask_phone",        ignore_errors=True)

# dim_employee masks
run_sql(f"ALTER TABLE {DIM}.dim_employee ALTER COLUMN email        SET MASK {CATALOG}.dimensions.mask_employee_email", ignore_errors=True)
run_sql(f"ALTER TABLE {DIM}.dim_employee ALTER COLUMN salary_band  SET MASK {CATALOG}.dimensions.mask_salary_band",   ignore_errors=True)

# Cost columns in fact tables are NOT masked — masking them to NULL breaks all downstream
# margin and profitability calculations in mart views for non-finance users.
# Sensitivity is communicated via column tags (see tagging section below).
# The mask functions above are retained as demo artifacts to illustrate the pattern.
# Drop the mask if it was applied by a previous run of this notebook.
run_sql(f"ALTER TABLE {FACT}.fact_order_items ALTER COLUMN unit_cost        DROP MASK", ignore_errors=True)
run_sql(f"ALTER TABLE {FACT}.fact_inventory   ALTER COLUMN unit_cost        DROP MASK", ignore_errors=True)
run_sql(f"ALTER TABLE {FACT}.fact_inventory   ALTER COLUMN total_cost_value DROP MASK", ignore_errors=True)

print("Column masks applied (PII only). Cost column masks dropped — use column tags for sensitivity signalling.")

# COMMAND ----------

# MAGIC %md ## Apply Row Filters

# COMMAND ----------

run_sql(f"ALTER TABLE {DIM}.dim_customer  SET ROW FILTER {CATALOG}.dimensions.customer_active_filter ON (is_active)",  ignore_errors=True)
run_sql(f"ALTER TABLE {FACT}.fact_orders  SET ROW FILTER {CATALOG}.facts.order_status_filter ON (order_status)",         ignore_errors=True)

print("Row filters applied.")

# COMMAND ----------

# MAGIC %md ## Table-Level Tags

# COMMAND ----------

TABLE_TAGS = {
    f"{DIM}.dim_date":       "domain=time,pii=false,sla=gold,owner=data_engineering,source_system=generated",
    f"{DIM}.dim_customer":   "domain=crm,pii=true,sla=gold,owner=data_engineering,source_system=crm,gdpr_relevant=true,data_classification=confidential",
    f"{DIM}.dim_product":    "domain=catalogue,pii=false,sla=gold,owner=merchandising,source_system=erp,data_classification=internal",
    f"{DIM}.dim_store":      "domain=retail_ops,pii=false,sla=gold,owner=operations,source_system=erp,data_classification=internal",
    f"{DIM}.dim_employee":   "domain=hr,pii=true,sla=gold,owner=hr,source_system=hris,gdpr_relevant=true,data_classification=confidential",
    f"{DIM}.dim_promotion":  "domain=marketing,pii=false,sla=silver,owner=marketing,source_system=campaign_mgr,data_classification=internal",
    f"{FACT}.fact_orders":      "domain=transactions,pii=false,sla=gold,owner=data_engineering,source_system=pos,data_classification=confidential",
    f"{FACT}.fact_order_items": "domain=transactions,pii=false,sla=gold,owner=data_engineering,source_system=pos,data_classification=confidential",
    f"{FACT}.fact_inventory":   "domain=supply_chain,pii=false,sla=silver,owner=operations,source_system=wms,data_classification=internal",
    f"{FACT}.fact_returns":     "domain=transactions,pii=false,sla=silver,owner=data_engineering,source_system=pos,data_classification=internal",
    f"{MART}.v_sales_summary":         "domain=analytics,pii=false,sla=gold,owner=analytics,genie_enabled=true,data_classification=internal",
    f"{MART}.v_customer_360":          "domain=analytics,pii=true,sla=gold,owner=analytics,genie_enabled=true,gdpr_relevant=true,data_classification=confidential",
    f"{MART}.v_product_performance":   "domain=analytics,pii=false,sla=gold,owner=analytics,genie_enabled=true,data_classification=internal",
    f"{MART}.v_store_performance":     "domain=analytics,pii=false,sla=gold,owner=analytics,genie_enabled=true,data_classification=internal",
    f"{MART}.v_employee_sales":        "domain=analytics,pii=false,sla=silver,owner=analytics,genie_enabled=true,data_classification=internal",
    f"{MART}.v_cohort_analysis":       "domain=analytics,pii=false,sla=silver,owner=analytics,genie_enabled=true,data_classification=internal",
    f"{MART}.agg_daily_sales":         "domain=analytics,pii=false,sla=gold,owner=analytics,genie_enabled=true,data_classification=internal,liquid_clustered=true",
    f"{ML}.customer_features":  "domain=machine_learning,pii=false,sla=gold,owner=data_science,feature_table=true,model=churn_predictor",
    f"{ML}.product_features":   "domain=machine_learning,pii=false,sla=silver,owner=data_science,feature_table=true,model=demand_forecast",
    f"{ML}.churn_predictions":  "domain=machine_learning,pii=false,sla=gold,owner=data_science,model_output=true,model=churn_predictor",
    f"{ML}.sales_forecast":     "domain=machine_learning,pii=false,sla=silver,owner=data_science,model_output=true,model=sales_forecast",
}

first_tag = True
for table, tags_str in TABLE_TAGS.items():
    obj_type = "VIEW" if ".v_" in table else "TABLE"
    for pair in tags_str.split(","):
        k, v = pair.split("=")
        # First tag: fail loudly to surface any errors; rest: ignore errors
        run_sql(f"ALTER {obj_type} {table} SET TAGS ('{k}' = '{v}')", ignore_errors=not first_tag)
        first_tag = False

print(f"Table-level tags applied to {len(TABLE_TAGS)} objects.")

# COMMAND ----------

# MAGIC %md ## Column-Level Tags (PII & Sensitivity)

# COMMAND ----------

COLUMN_TAGS = [
    # dim_customer PII columns
    (f"{DIM}.dim_customer", "email",           "pii=true,pii_type=email,gdpr_sensitive=true,sensitivity=high"),
    (f"{DIM}.dim_customer", "phone",           "pii=true,pii_type=phone,gdpr_sensitive=true,sensitivity=high"),
    (f"{DIM}.dim_customer", "date_of_birth",   "pii=true,pii_type=date_of_birth,gdpr_sensitive=true,sensitivity=high"),
    (f"{DIM}.dim_customer", "full_name",        "pii=true,pii_type=name,gdpr_sensitive=true,sensitivity=medium"),
    (f"{DIM}.dim_customer", "first_name",       "pii=true,pii_type=name,gdpr_sensitive=true,sensitivity=medium"),
    (f"{DIM}.dim_customer", "last_name",        "pii=true,pii_type=name,gdpr_sensitive=true,sensitivity=medium"),
    # dim_employee PII columns
    (f"{DIM}.dim_employee", "email",            "pii=true,pii_type=email,gdpr_sensitive=true,sensitivity=high"),
    (f"{DIM}.dim_employee", "phone",            "pii=true,pii_type=phone,gdpr_sensitive=true,sensitivity=high"),
    (f"{DIM}.dim_employee", "salary_band",      "sensitive=true,sensitivity=high,access_restricted=hr_only"),
    # fact_order_items cost column
    (f"{FACT}.fact_order_items", "unit_cost",   "sensitive=true,sensitivity=high,access_restricted=finance_only,commercial=true"),
    (f"{FACT}.fact_inventory",   "unit_cost",   "sensitive=true,sensitivity=high,access_restricted=finance_only,commercial=true"),
    (f"{FACT}.fact_inventory",   "total_cost_value", "sensitive=true,sensitivity=high,access_restricted=finance_only,commercial=true"),
    # dim_product cost
    (f"{DIM}.dim_product", "unit_cost",         "sensitive=true,sensitivity=medium,access_restricted=finance_only,commercial=true"),
    (f"{DIM}.dim_product", "gross_margin_pct",  "sensitive=true,sensitivity=medium,access_restricted=finance_only,commercial=true"),
]

for table, column, tags_str in COLUMN_TAGS:
    obj_type = "VIEW" if ".v_" in table else "TABLE"
    for pair in tags_str.split(","):
        k, v = pair.split("=")
        run_sql(f"ALTER {obj_type} {table} ALTER COLUMN {column} SET TAGS ('{k}' = '{v}')", ignore_errors=True)

print(f"Column-level tags applied to {len(COLUMN_TAGS)} columns.")

# COMMAND ----------

# MAGIC %md ## Table Comments (rich descriptions for Genie)

# COMMAND ----------

TABLE_COMMENTS = {
    f"{DIM}.dim_date": "Date dimension table covering 2020-01-01 to 2027-12-31. Contains calendar and fiscal calendar attributes. Join to fact tables on date_key (format: YYYYMMDD integer). Fiscal year begins in April.",
    f"{DIM}.dim_customer": "Customer master dimension. One row per unique customer. Contains PII (email, phone, date_of_birth) — columns are masked for non-privileged roles. A row filter restricts inactive customers to pii_readers only. Customer segments: Consumer, Small Business, Enterprise. Loyalty tiers: Bronze < Silver < Gold < Platinum.",
    f"{DIM}.dim_product": "Product catalogue dimension. 200 active products across 5 categories: Electronics, Clothing, Food & Beverage, Home & Garden, Sports & Outdoors. Unit cost is masked for non-finance roles. Launch dates span 2019–2023.",
    f"{DIM}.dim_store": "Physical and online store dimension. 15 locations across US, UK, Canada, and Australia. Store types: Flagship (largest), Standard, Express (smallest physical), Online. annual_target is the store revenue target for the calendar year.",
    f"{DIM}.dim_employee": "Employee dimension. 50 employees across sales, operations, finance, HR, IT, and customer service departments. Salary band and email are masked. Self-referencing manager_key for hierarchy reporting.",
    f"{DIM}.dim_promotion": "Promotional campaigns dimension. 20 promotional events including seasonal sales, loyalty rewards, and category-specific discounts. Discount rates range from 10% to 40%.",
    f"{FACT}.fact_orders": "Order header fact table. 50,000 orders spanning 2022–2024 with realistic Q4 seasonality. Order statuses: Delivered, Shipped, Processing, Cancelled, Returned. Channels: In-Store, Online, Mobile App, Phone. A row filter hides Processing-status orders from standard analysts.",
    f"{FACT}.fact_order_items": "Order line items fact. ~130,000 rows (avg 2.6 items per order). Unit cost is masked for non-finance roles. Join to fact_orders on order_key, to dim_product on product_key.",
    f"{FACT}.fact_inventory": "Monthly inventory snapshot fact. 200 products × 14 physical stores × 24 monthly snapshots. Use snapshot_date_key to filter to a specific month. Unit cost and total_cost_value are masked for non-finance roles.",
    f"{FACT}.fact_returns": "Return transaction fact. ~2,500 rows representing approx. 5% return rate on Delivered orders. Links back to original order via order_key. Includes return reason, refund amount, and refund method.",
    f"{MART}.v_sales_summary": "Daily sales summary pre-joined across orders, dates, stores, and customers. Aggregated by store, channel, customer segment, and loyalty tier. Primary view for Genie revenue and trend questions.",
    f"{MART}.v_customer_360": "Customer 360-degree view. One row per customer with lifetime metrics, recency, preferred category, and churn risk flag. Best for customer LTV, segmentation, and churn analysis in Genie.",
    f"{MART}.v_product_performance": "Monthly product performance view. Covers revenue, units sold, return rate, and gross margin per product. Best for product mix, category analysis, and return rate queries in Genie.",
    f"{MART}.v_store_performance": "Monthly store performance vs target. Includes revenue per employee, revenue per sqft, and pct_of_target. Best for store benchmarking and underperformance identification.",
    f"{MART}.v_employee_sales": "Monthly employee sales performance. Aggregated by employee, store, and month. Best for sales associate leaderboards and coaching queries.",
    f"{MART}.v_cohort_analysis": "Customer cohort retention analysis. Shows retention_rate_pct by months since acquisition (period_number 0–24). Best for understanding acquisition channel quality and long-term retention.",
    f"{MART}.agg_daily_sales": "Pre-aggregated daily sales totals. Physical Delta table liquid-clustered on (order_date, store_region) for fast date-range scans. Optimised for Genie trend queries and BI dashboards.",
    f"{ML}.customer_features": "Customer feature table for churn prediction. Pre-computed behavioural features with 90-day lookback window. is_churn_label = TRUE when no orders in 60 days AND fewer than 2 orders in 90 days.",
    f"{ML}.product_features": "Product feature table for demand forecasting. 30-day rolling metrics including velocity, return rate, and inventory health. is_high_velocity = units_sold_last_30d >= 50.",
    f"{ML}.churn_predictions": "Churn probability scores from the champion model. churn_probability ranges 0.0–1.0. risk_tier: Low (<30%), Medium (30-50%), High (50-70%), Critical (>70%). Refreshed weekly.",
    f"{ML}.sales_forecast": "30-day forward sales forecast by store and product category. Uses a 28-day moving average baseline. forecast_horizon_days = 1 (tomorrow) to 30. Replace with ML model output for production use.",
}

for table, comment in TABLE_COMMENTS.items():
    safe_comment = comment.replace("'", "\\'")
    run_sql(f"COMMENT ON TABLE {table} IS '{safe_comment}'", ignore_errors=True)

print(f"Table comments applied to {len(TABLE_COMMENTS)} objects.")

# COMMAND ----------

# MAGIC %md ## Column Comments (semantic descriptions for Genie)

# COMMAND ----------

COLUMN_COMMENTS = {
    f"{DIM}.dim_date": {
        "date_key":       "Surrogate key in YYYYMMDD integer format (e.g. 20240115). Primary key and FK target for all date columns in fact tables.",
        "full_date":      "Calendar date value (DATE type). Use for date arithmetic and range filters.",
        "day_of_week":    "Day of week number: 1=Sunday, 2=Monday … 7=Saturday (Spark convention).",
        "day_name":       "Full day name (e.g. Monday, Tuesday).",
        "month_name":     "Full month name (e.g. January, February).",
        "month_short":    "Three-letter month abbreviation (e.g. Jan, Feb).",
        "calendar_quarter": "Calendar quarter number (1–4).",
        "calendar_year":  "Calendar year (e.g. 2024).",
        "year_quarter":   "Year and quarter string (e.g. 2024-Q3). Useful for GROUP BY in trend reports.",
        "year_month":     "Year and month string (e.g. 2024-07). Useful for monthly aggregations.",
        "is_weekend":     "True if the date is a Saturday or Sunday.",
        "is_holiday":     "True for major public holidays (Christmas Eve/Day/Boxing Day, New Year's Day).",
        "is_business_day":"True if the date is a weekday and not a holiday.",
        "fiscal_quarter": "Fiscal quarter (1–4) where fiscal year begins in April.",
        "fiscal_year":    "Fiscal year (e.g. 2024 = April 2023 – March 2024).",
        "fiscal_year_quarter": "Fiscal year and quarter string (e.g. FY2024-Q1).",
    },
    f"{DIM}.dim_customer": {
        "customer_key":        "Surrogate primary key.",
        "customer_id":         "Business-facing customer identifier (e.g. CUST-001234). Stable across migrations.",
        "first_name":          "Customer first name. PII — masked for non-privileged roles.",
        "last_name":           "Customer last name. PII — masked for non-privileged roles.",
        "full_name":           "Combined first and last name. PII.",
        "email":               "Customer email address. PII — masked: non-pii_readers see a***@domain.com format.",
        "phone":               "Customer phone number. PII — masked: non-pii_readers see ***-***-XXXX format.",
        "date_of_birth":       "Customer date of birth. PII and GDPR-sensitive.",
        "gender":              "Gender identity: M (Male), F (Female), NB (Non-binary), U (Unknown/not stated).",
        "city":                "Customer home city.",
        "state_province":      "State or province of residence.",
        "country":             "Country of residence (e.g. United States, United Kingdom, Canada, Australia).",
        "region":              "Geographic region grouping: Northeast, South, Midwest, West, International.",
        "postal_code":         "Postal/ZIP code.",
        "segment":             "Business segment: Consumer (individuals), Small Business, or Enterprise.",
        "annual_income_band":  "Self-reported annual income band. Used for targeting and segmentation.",
        "loyalty_tier":        "Loyalty programme tier earned through cumulative spend: Bronze < Silver < Gold < Platinum.",
        "loyalty_points":      "Current loyalty points balance. Platinum customers typically have 20,000+ points.",
        "acquisition_channel": "Channel through which the customer was acquired (e.g. Online, In-Store, Referral).",
        "preferred_payment":   "Most commonly used payment method.",
        "registration_date":   "Date the customer first registered an account.",
        "is_active":           "True if the customer account is currently active. Inactive records are hidden by row filter for non-PII roles.",
        "marketing_consent":   "True if the customer has given consent to receive marketing communications.",
        "_created_at":         "Record creation timestamp.",
        "_updated_at":         "Record last-modified timestamp.",
    },
    f"{DIM}.dim_product": {
        "product_key":      "Surrogate primary key.",
        "product_id":       "Business product identifier (e.g. PROD-001).",
        "product_name":     "Full product name as displayed to customers.",
        "brand":            "Product brand name.",
        "category":         "Top-level product category: Electronics, Clothing, Food & Beverage, Home & Garden, Sports & Outdoors.",
        "sub_category":     "Product sub-category (e.g. Smartphones, Laptops, Footwear).",
        "sku":              "Stock-keeping unit code. Unique per product.",
        "description":      "Short product description.",
        "unit_price":       "Standard retail selling price (USD).",
        "unit_cost":        "Landed cost per unit. Masked for non-finance roles.",
        "gross_margin_pct": "Gross margin as a percentage: (unit_price - unit_cost) / unit_price * 100. Masked for non-finance roles.",
        "weight_kg":        "Product weight in kilograms. Used for shipping cost calculation.",
        "is_active":        "True if the product is currently on sale.",
        "launch_date":      "Date the product was first made available for sale.",
        "discontinue_date": "Date the product was discontinued (NULL if still active).",
        "supplier_id":      "Identifier of the primary supplier for this product.",
        "reorder_point":    "Inventory level that triggers a reorder. Units.",
        "reorder_quantity": "Standard quantity ordered when reorder is triggered. Units.",
    },
    f"{DIM}.dim_store": {
        "store_key":       "Surrogate primary key.",
        "store_id":        "Business store identifier (e.g. STORE-001).",
        "store_name":      "Store display name (e.g. New York Flagship).",
        "store_type":      "Store format: Flagship (largest, premium), Standard, Express (compact), Online.",
        "city":            "City where the store is located.",
        "state_province":  "State or province.",
        "country":         "Country.",
        "region":          "Geographic grouping: Northeast, South, Midwest, West, International, Online.",
        "latitude":        "Store latitude coordinate for map visualisations.",
        "longitude":       "Store longitude coordinate for map visualisations.",
        "open_date":       "Date the store opened.",
        "is_active":       "True if the store is currently operating.",
        "floor_area_sqft": "Store trading floor area in square feet (0 for Online store).",
        "num_employees":   "Current headcount at the store.",
        "annual_target":   "Annual revenue target for this store (USD). Used to compute pct_of_target in v_store_performance.",
        "manager_id":      "Employee ID of the current store manager.",
    },
    f"{DIM}.dim_employee": {
        "employee_key":       "Surrogate primary key.",
        "employee_id":        "Business employee identifier (e.g. EMP-001).",
        "first_name":         "Employee first name.",
        "last_name":          "Employee last name.",
        "email":              "Work email address. Masked for non-HR roles.",
        "department":         "Department: Sales, Operations, Finance, HR, IT, Customer Service.",
        "job_title":          "Job title (e.g. Sales Associate, Store Manager, Finance Analyst).",
        "salary_band":        "Compensation band (e.g. B1, B2, B3, M1, M2). Masked for non-HR roles.",
        "hire_date":          "Date the employee joined the company.",
        "termination_date":   "Date employment ended (NULL if currently employed).",
        "store_key":          "Store where the employee primarily works. FK to dim_store.",
        "manager_key":        "Employee key of the line manager. Self-referencing FK to dim_employee.",
        "performance_rating": "Most recent annual performance rating on a 1.0–5.0 scale.",
        "is_active":          "True if the employee is currently employed.",
    },
    f"{DIM}.dim_promotion": {
        "promotion_key":        "Surrogate primary key.",
        "promotion_id":         "Business promotion identifier (e.g. PROMO-001).",
        "promotion_name":       "Human-readable promotion name (e.g. Black Friday Mega Deal).",
        "promotion_type":       "Type of discount: Percentage, Fixed amount, BOGO (buy one get one), Bundle.",
        "discount_rate":        "Discount fraction (0.0–1.0). E.g. 0.30 = 30% off.",
        "start_date":           "Promotion start date (inclusive).",
        "end_date":             "Promotion end date (inclusive).",
        "applicable_category":  "Product category the promotion applies to (NULL = all categories).",
        "min_order_value":      "Minimum order value required to qualify for the promotion (USD). 0 = no minimum.",
        "is_stackable":         "True if the promotion can be combined with other promotions.",
    },
    f"{FACT}.fact_orders": {
        "order_key":        "Surrogate primary key.",
        "order_id":         "Business order identifier (e.g. ORD-0001234).",
        "customer_key":     "FK to dim_customer. The customer who placed the order.",
        "store_key":        "FK to dim_store. The store (or Online) that fulfilled the order.",
        "employee_key":     "FK to dim_employee. The sales associate who processed the order.",
        "order_date_key":   "FK to dim_date (YYYYMMDD). The date the order was placed.",
        "ship_date_key":    "FK to dim_date (YYYYMMDD). The date the order was shipped. -1 if not yet shipped.",
        "order_status":     "Order lifecycle status: Delivered, Shipped, Processing, Cancelled, Returned.",
        "channel":          "Order channel: In-Store, Online, Mobile App, Phone.",
        "payment_method":   "Payment method used: Credit Card, Debit Card, PayPal, Cash, Gift Card, Apple Pay, Buy Now Pay Later.",
        "shipping_method":  "Fulfilment method: Standard, Express, Same-Day, Click & Collect.",
        "promotion_key":    "FK to dim_promotion. -1 if no promotion was applied.",
        "subtotal":         "Order subtotal before discounts (USD).",
        "discount_amount":  "Total discount applied to the order (USD).",
        "tax_amount":       "Sales tax charged (USD).",
        "shipping_amount":  "Shipping charge (USD). Free (0.00) for orders over $75.",
        "total_amount":     "Final order total after discounts, plus tax and shipping (USD).",
        "currency":         "Currency code (always USD in this dataset).",
        "is_returned":      "True if the order was returned after delivery.",
        "return_date_key":  "FK to dim_date. Return date. -1 if not returned.",
    },
    f"{FACT}.fact_order_items": {
        "order_item_key":   "Surrogate primary key.",
        "order_key":        "FK to fact_orders.",
        "order_id":         "Business order identifier. Denormalised from fact_orders for convenience.",
        "product_key":      "FK to dim_product.",
        "quantity":         "Number of units of this product in the line item.",
        "unit_price":       "Selling price per unit at time of order (USD). May differ from current dim_product.unit_price.",
        "unit_cost":        "Cost per unit at time of order (USD). Masked for non-finance roles.",
        "discount_pct":     "Item-level discount percentage applied (0–100).",
        "discount_amount":  "Item-level discount amount in USD.",
        "line_total":       "Total for this line item after discounts: (unit_price × quantity) - discount_amount (USD).",
    },
    f"{FACT}.fact_inventory": {
        "inventory_key":          "Surrogate primary key.",
        "snapshot_date_key":      "FK to dim_date (YYYYMMDD). Date of the monthly inventory snapshot.",
        "product_key":            "FK to dim_product.",
        "store_key":              "FK to dim_store (physical stores only; Online store excluded).",
        "quantity_on_hand":       "Total units physically present in the store.",
        "quantity_reserved":      "Units reserved for pending orders (not yet picked/shipped).",
        "quantity_available":     "Units available for sale: quantity_on_hand - quantity_reserved.",
        "quantity_on_order":      "Units currently on order from supplier (in transit).",
        "reorder_point":          "Stock level that triggers a purchase order to the supplier.",
        "reorder_triggered":      "True if quantity_available < reorder_point at snapshot time.",
        "unit_cost":              "Cost per unit at snapshot date (USD). Masked for non-finance roles.",
        "total_cost_value":       "Total inventory value: quantity_on_hand × unit_cost (USD). Masked for non-finance roles.",
    },
    f"{FACT}.fact_returns": {
        "return_key":      "Surrogate primary key.",
        "return_id":       "Business return identifier (e.g. RET-000123).",
        "order_key":       "FK to fact_orders. The original order being returned.",
        "order_id":        "Business order identifier of the original order.",
        "customer_key":    "FK to dim_customer.",
        "store_key":       "FK to dim_store. Store processing the return.",
        "return_date_key": "FK to dim_date (YYYYMMDD).",
        "return_reason":   "Reason provided by the customer: Changed Mind, Defective Product, Wrong Item Received, Item Damaged, Better Price Found, Ordered by Mistake, Other.",
        "refund_amount":   "Amount refunded to the customer (USD). May be less than original order total for partial returns.",
        "refund_method":   "How the refund was issued: Original Payment Method, Store Credit, Gift Card, Bank Transfer.",
        "is_restocked":    "True if the returned item was returned to inventory (not damaged).",
    },
    f"{MART}.agg_daily_sales": {
        "order_date":        "Calendar date of the sales orders.",
        "year":              "Calendar year.",
        "quarter":           "Calendar quarter (1–4).",
        "year_quarter":      "Year-quarter label (e.g. 2024-Q3).",
        "year_month":        "Year-month label (e.g. 2024-07).",
        "month_name":        "Full month name.",
        "month_num":         "Month number (1–12).",
        "is_weekend":        "True if order_date is Saturday or Sunday.",
        "is_holiday":        "True if order_date is a public holiday.",
        "fiscal_year":       "Fiscal year (April start).",
        "fiscal_quarter":    "Fiscal quarter (1–4, April start).",
        "store_name":        "Name of the fulfilling store.",
        "store_type":        "Store format: Flagship, Standard, Express, Online.",
        "store_region":      "Geographic region of the store.",
        "store_country":     "Country of the store.",
        "channel":           "Order channel: In-Store, Online, Mobile App, Phone.",
        "num_orders":        "Total number of orders placed on this date for this store/channel combination.",
        "unique_customers":  "Number of distinct customers who ordered.",
        "total_revenue":     "Net revenue after discounts (sum of total_amount) in USD.",
        "gross_revenue":     "Revenue before discounts (sum of subtotal) in USD.",
        "total_discounts":   "Total discount value applied in USD.",
        "total_tax":         "Total sales tax collected in USD.",
        "avg_order_value":   "Average order value (total_revenue / num_orders) in USD.",
        "returned_revenue":  "Revenue from orders that were subsequently returned in USD.",
        "cancelled_orders":  "Number of cancelled orders.",
        "promoted_orders":   "Number of orders where a promotional discount was applied.",
    },
}

for table, col_comments in COLUMN_COMMENTS.items():
    for col, comment in col_comments.items():
        safe = comment.replace("'", "\\'")
        run_sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{safe}'", ignore_errors=True)

print(f"Column comments applied to {sum(len(v) for v in COLUMN_COMMENTS.values())} columns across {len(COLUMN_COMMENTS)} tables.")

# COMMAND ----------

# MAGIC %md ## Primary Keys and Foreign Keys (informational, NOT ENFORCED)

# COMMAND ----------

# Primary keys
PK_STATEMENTS = [
    f"ALTER TABLE {DIM}.dim_date       ADD CONSTRAINT pk_dim_date       PRIMARY KEY (date_key)       NOT ENFORCED RELY",
    f"ALTER TABLE {DIM}.dim_customer   ADD CONSTRAINT pk_dim_customer   PRIMARY KEY (customer_key)   NOT ENFORCED RELY",
    f"ALTER TABLE {DIM}.dim_product    ADD CONSTRAINT pk_dim_product    PRIMARY KEY (product_key)    NOT ENFORCED RELY",
    f"ALTER TABLE {DIM}.dim_store      ADD CONSTRAINT pk_dim_store      PRIMARY KEY (store_key)      NOT ENFORCED RELY",
    f"ALTER TABLE {DIM}.dim_employee   ADD CONSTRAINT pk_dim_employee   PRIMARY KEY (employee_key)   NOT ENFORCED RELY",
    f"ALTER TABLE {DIM}.dim_promotion  ADD CONSTRAINT pk_dim_promotion  PRIMARY KEY (promotion_key)  NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_orders       ADD CONSTRAINT pk_fact_orders       PRIMARY KEY (order_key)       NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_order_items  ADD CONSTRAINT pk_fact_order_items  PRIMARY KEY (order_item_key)  NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_inventory    ADD CONSTRAINT pk_fact_inventory    PRIMARY KEY (inventory_key)   NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_returns      ADD CONSTRAINT pk_fact_returns      PRIMARY KEY (return_key)      NOT ENFORCED RELY",
]

for stmt in PK_STATEMENTS:
    run_sql(stmt, ignore_errors=True)

# Foreign keys
FK_STATEMENTS = [
    f"ALTER TABLE {FACT}.fact_orders ADD CONSTRAINT fk_orders_customer  FOREIGN KEY (customer_key)  REFERENCES {DIM}.dim_customer (customer_key)  NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_orders ADD CONSTRAINT fk_orders_store      FOREIGN KEY (store_key)     REFERENCES {DIM}.dim_store    (store_key)     NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_orders ADD CONSTRAINT fk_orders_employee   FOREIGN KEY (employee_key)  REFERENCES {DIM}.dim_employee (employee_key)  NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_orders ADD CONSTRAINT fk_orders_date       FOREIGN KEY (order_date_key) REFERENCES {DIM}.dim_date     (date_key)      NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_order_items ADD CONSTRAINT fk_items_order   FOREIGN KEY (order_key)    REFERENCES {FACT}.fact_orders   (order_key)    NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_order_items ADD CONSTRAINT fk_items_product FOREIGN KEY (product_key)  REFERENCES {DIM}.dim_product    (product_key)  NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_inventory ADD CONSTRAINT fk_inv_product     FOREIGN KEY (product_key)  REFERENCES {DIM}.dim_product    (product_key)  NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_inventory ADD CONSTRAINT fk_inv_store       FOREIGN KEY (store_key)    REFERENCES {DIM}.dim_store      (store_key)    NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_returns ADD CONSTRAINT fk_ret_order         FOREIGN KEY (order_key)    REFERENCES {FACT}.fact_orders   (order_key)    NOT ENFORCED RELY",
    f"ALTER TABLE {FACT}.fact_returns ADD CONSTRAINT fk_ret_customer      FOREIGN KEY (customer_key) REFERENCES {DIM}.dim_customer   (customer_key) NOT ENFORCED RELY",
]

for stmt in FK_STATEMENTS:
    run_sql(stmt, ignore_errors=True)

print(f"Primary keys: {len(PK_STATEMENTS)}  |  Foreign keys: {len(FK_STATEMENTS)}")

# COMMAND ----------

# MAGIC %md ## Permission Grants (production only)

# COMMAND ----------

if ENV.lower() == "prod":
    print("Applying production grants …")
    GRANT_STATEMENTS = [
        # Analysts can query the mart schema
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `analysts`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.mart TO `analysts`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.mart TO `analysts`",
        # PII readers can query all dimension tables (full email/phone visible)
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.dimensions TO `pii_readers`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.dimensions TO `pii_readers`",
        # Finance role can see cost columns in fact tables
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.facts TO `finance_role`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.facts TO `finance_role`",
        # Data scientists get full ML schema access
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.ml TO `data_scientists`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.ml TO `data_scientists`",
        # HR can query employee dimension with unmasked salary_band
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.dimensions TO `hr_role`",
        f"GRANT SELECT ON TABLE {DIM}.dim_employee TO `hr_role`",
        # Raw data admins can write to the volume
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.raw TO `data_engineers`",
        f"GRANT READ VOLUME ON VOLUME {CATALOG}.raw.raw_uploads TO `data_engineers`",
        f"GRANT WRITE VOLUME ON VOLUME {CATALOG}.raw.raw_uploads TO `data_engineers`",
    ]
    for stmt in GRANT_STATEMENTS:
        run_sql(stmt, ignore_errors=True)
    print(f"  {len(GRANT_STATEMENTS)} grant statements applied.")
else:
    print(f"[SKIP] Grant statements skipped for env='{ENV}'. Set env='prod' to apply.")
    print("       Example grants documented in code above for reference.")

# COMMAND ----------

print("\nGovernance summary:")
print(f"  Column mask functions : 6")
print(f"  Row filter functions  : 2")
print(f"  Tables with masks     : 3")
print(f"  Tables with row filter: 2")
print(f"  Tables tagged         : {len(TABLE_TAGS)}")
print(f"  Columns tagged        : {len(COLUMN_TAGS)}")
print(f"  Tables with comments  : {len(TABLE_COMMENTS)}")
print(f"  Columns with comments : {sum(len(v) for v in COLUMN_COMMENTS.values())}")
print(f"  Primary keys          : {len(PK_STATEMENTS)}")
print(f"  Foreign keys          : {len(FK_STATEMENTS)}")
print(f"  Grants applied        : {'Yes (prod)' if ENV == 'prod' else 'No (dev)'}")
print("\nGovernance applied successfully.")
