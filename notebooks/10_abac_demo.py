# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 — Attribute-Based Access Control (ABAC) Demo
# MAGIC
# MAGIC Demonstrates **Unity Catalog ABAC** using the Northwind Analytics sample database.
# MAGIC
# MAGIC | Section | What It Shows |
# MAGIC |---------|--------------|
# MAGIC | **A — Identity Functions** | `current_user()`, `is_account_group_member()`, `is_member()` |
# MAGIC | **B — Column Masks** | Email, phone, salary, and cost masking with group-based reveal |
# MAGIC | **C — Row Filters** | Region-based and active-status row-level security |
# MAGIC | **D — Column Tags** | Tagging columns as PII, financial, or sensitive |
# MAGIC | **E — ABAC Policies** | Tag-driven policies that auto-apply to all matching columns |
# MAGIC | **F — Audit & Introspection** | `information_schema` views for governance reporting |
# MAGIC | **G — Demo Scenarios** | Side-by-side comparisons for live demos |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Northwind Analytics catalog deployed (run `00_main.py` first)
# MAGIC - DBR 12.2 LTS+ for manual filters/masks (sections A–D, F)
# MAGIC - DBR 16.4+ or Serverless for ABAC Policies (section E)
# MAGIC - `CREATE FUNCTION` privilege on the catalog schemas
# MAGIC - `MANAGE` privilege on `dim_customer`, `dim_employee`, `fact_order_items`
# MAGIC
# MAGIC ## Documentation
# MAGIC See `ABAC_DEMO.md` in the repo root for full reference and documentation links.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "northwind", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

DIM  = f"{CATALOG}.dimensions"
FACT = f"{CATALOG}.facts"
MART = f"{CATALOG}.mart"

print(f"Targeting catalog: {CATALOG}")
print(f"  Dimensions schema : {DIM}")
print(f"  Facts schema      : {FACT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section A — Identity Functions
# MAGIC
# MAGIC These SQL built-ins are the building blocks of all row filters and column masks.
# MAGIC They are evaluated **at query time** for every row read, making them suitable for
# MAGIC dynamic, user-aware access decisions.

# COMMAND ----------

# MAGIC %md ### A1 — `current_user()` and `session_user()`

# COMMAND ----------

# SQL: who am I?
display(spark.sql("SELECT current_user() AS session_user, session_user() AS session_user_alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC > `current_user()` returns the email of the logged-in user.
# MAGIC > `session_user()` is the preferred form in DBR 14.1+ — it also handles service principals (returns UUID).

# COMMAND ----------

# MAGIC %md ### A2 — `is_account_group_member()` vs `is_member()`

# COMMAND ----------

# SQL: check group membership
spark.sql("""
SELECT
  current_user()                                   AS user,
  is_account_group_member('account_admins')        AS is_account_admin,
  is_account_group_member('pii_readers')           AS is_pii_reader,
  is_account_group_member('finance_role')          AS is_finance,
  is_account_group_member('hr_role')               AS is_hr,
  is_member('analysts')                            AS is_workspace_analyst
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC | Function | Group Type | Use Case |
# MAGIC |----------|-----------|---------|
# MAGIC | `is_account_group_member()` | Account-level Unity Catalog groups | **Recommended** for UC governance policies |
# MAGIC | `is_member()` | Workspace-local OR account groups assigned to workspace | Legacy Hive Metastore, backward compat |
# MAGIC
# MAGIC > **Rule of thumb:** Use `is_account_group_member()` for all new Unity Catalog row filters and column masks.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section B — Column Masks (Dynamic Data Masking)
# MAGIC
# MAGIC A column mask is a **SQL UDF** whose return value replaces the raw column value.
# MAGIC The first parameter maps 1:1 to the masked column.
# MAGIC The return type must be castable to the column's data type.

# COMMAND ----------

# MAGIC %md ### B1 — Create Mask UDFs

# COMMAND ----------

def run_sql(stmt, label=""):
    """Execute SQL and print a confirmation message."""
    try:
        spark.sql(stmt)
        if label:
            print(f"  [OK] {label}")
    except Exception as e:
        print(f"  [WARN] {label}: {str(e)[:150]}")

# COMMAND ----------

# Email mask: first char + ***@domain  (non-pii_readers)
run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.demo_mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'ABAC Demo: masks customer email for non-pii_readers.
         pii_readers and account_admins see the full address.
         Others see: a***@domain.com'
RETURN
  CASE
    WHEN is_account_group_member('pii_readers')    THEN email
    WHEN is_account_group_member('account_admins') THEN email
    WHEN email IS NULL                             THEN NULL
    ELSE CONCAT(LEFT(email, 1), '***@', split_part(email, '@', 2))
  END
""", "Email mask UDF created")

# Phone mask: keep last 4 digits (non-pii_readers)
run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.demo_mask_phone(phone STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'ABAC Demo: masks phone number for non-pii_readers.
         Others see: ***-***-XXXX'
RETURN
  CASE
    WHEN is_account_group_member('pii_readers')    THEN phone
    WHEN is_account_group_member('account_admins') THEN phone
    WHEN phone IS NULL                             THEN NULL
    ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4))
  END
""", "Phone mask UDF created")

# Salary band mask: HR only
run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.demo_mask_salary_band(salary_band STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'ABAC Demo: salary bands are visible only to hr_role and account_admins'
RETURN
  CASE
    WHEN is_account_group_member('hr_role')        THEN salary_band
    WHEN is_account_group_member('account_admins') THEN salary_band
    ELSE 'REDACTED'
  END
""", "Salary band mask UDF created")

# Unit cost mask: finance only (returns NULL for non-finance to preserve numeric type)
run_sql(f"""
CREATE OR REPLACE FUNCTION {FACT}.demo_mask_unit_cost(unit_cost DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
DETERMINISTIC
COMMENT 'ABAC Demo: unit cost (COGS) visible only to finance_role and account_admins.
         Others receive NULL — prevents margin calculation by unauthorised users.'
RETURN
  CASE
    WHEN is_account_group_member('finance_role')   THEN unit_cost
    WHEN is_account_group_member('account_admins') THEN unit_cost
    ELSE NULL
  END
""", "Unit cost mask UDF created")

# COMMAND ----------

# MAGIC %md ### B2 — Inspect a Mask UDF

# COMMAND ----------

# SQL: describe the UDF so the audience can see the logic
spark.sql(f"DESCRIBE FUNCTION EXTENDED {DIM}.demo_mask_email").display()

# COMMAND ----------

# MAGIC %md ### B3 — Apply Masks to Tables

# COMMAND ----------

# Apply email and phone masks to dim_customer
run_sql(
    f"ALTER TABLE {DIM}.dim_customer ALTER COLUMN email SET MASK {DIM}.demo_mask_email",
    "Email mask applied to dim_customer.email"
)
run_sql(
    f"ALTER TABLE {DIM}.dim_customer ALTER COLUMN phone SET MASK {DIM}.demo_mask_phone",
    "Phone mask applied to dim_customer.phone"
)

# Apply salary band mask to dim_employee
run_sql(
    f"ALTER TABLE {DIM}.dim_employee ALTER COLUMN salary_band SET MASK {DIM}.demo_mask_salary_band",
    "Salary band mask applied to dim_employee.salary_band"
)

# Apply unit cost mask to fact_order_items
run_sql(
    f"ALTER TABLE {FACT}.fact_order_items ALTER COLUMN unit_cost SET MASK {FACT}.demo_mask_unit_cost",
    "Unit cost mask applied to fact_order_items.unit_cost"
)

# COMMAND ----------

# MAGIC %md ### B4 — Verify Masking in Action

# COMMAND ----------

# SQL: observe the masked output
print("=== dim_customer: email and phone (masked for current user) ===")
spark.sql(f"""
    SELECT customer_id, first_name, last_name, email, phone, loyalty_tier
    FROM {DIM}.dim_customer
    ORDER BY customer_id
    LIMIT 8
""").display()

# COMMAND ----------

# Note: The values you see depend on which groups the current user belongs to.
# - pii_readers / account_admins → full email and phone
# - all others                   → a***@domain.com  and  ***-***-1234

current_user = spark.sql("SELECT current_user()").collect()[0][0]
is_pii = spark.sql("SELECT is_account_group_member('pii_readers')").collect()[0][0]
print(f"Current user      : {current_user}")
print(f"In pii_readers?   : {is_pii}")
print("Expected output   :", "FULL PII visible" if is_pii else "MASKED PII (a***@domain.com)")

# COMMAND ----------

print("=== fact_order_items: unit_cost (NULL for non-finance_role) ===")
spark.sql(f"""
    SELECT order_item_id, product_id, quantity, unit_price, unit_cost,
           (unit_price - unit_cost) AS gross_margin
    FROM {FACT}.fact_order_items
    ORDER BY order_item_id
    LIMIT 8
""").display()

# COMMAND ----------

# Explain the gross_margin implication
is_finance = spark.sql("SELECT is_account_group_member('finance_role')").collect()[0][0]
print(f"In finance_role? : {is_finance}")
print("Expected output  :", "FULL cost data" if is_finance else "unit_cost=NULL → gross_margin=NULL (cannot reverse-engineer margins)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section C — Row Filters
# MAGIC
# MAGIC A row filter is a **SQL UDF returning BOOLEAN**.
# MAGIC Rows where the UDF returns `FALSE` or `NULL` are **silently excluded** from all
# MAGIC query results — the caller sees a smaller table with no error.

# COMMAND ----------

# MAGIC %md ### C1 — Create Row Filter UDFs

# COMMAND ----------

# Filter 1: active customers only (boolean column)
run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.demo_filter_active_customers(is_active BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT 'ABAC Demo: hide inactive customers from non-admin users.
         Admins bypass; all others only see is_active = TRUE rows.'
RETURN
  CASE
    WHEN is_account_group_member('account_admins') THEN TRUE  -- admins see all
    ELSE is_active = TRUE
  END
""", "Active-customer row filter UDF created")

# Filter 2: store region restriction (applied to dim_store which has the region column)
run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.demo_filter_store_region(region STRING)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT 'ABAC Demo: restricts visible rows by region based on group membership.
         account_admins and global_analysts see all regions.
         west_analysts see only West; east_analysts see only East, etc.
         Applied to dim_store; cascades through joins to fact tables.'
RETURN
  CASE
    WHEN is_account_group_member('account_admins')   THEN TRUE
    WHEN is_account_group_member('global_analysts')  THEN TRUE
    WHEN is_account_group_member('west_analysts')    THEN region = 'West'
    WHEN is_account_group_member('east_analysts')    THEN region = 'East'
    WHEN is_account_group_member('central_analysts') THEN region = 'Central'
    ELSE TRUE  -- default allow (change to FALSE for deny-by-default posture)
  END
""", "Store-region row filter UDF created")

# COMMAND ----------

# MAGIC %md ### C2 — Apply Row Filters to Tables

# COMMAND ----------

# Apply active-customer filter to dim_customer
run_sql(
    f"ALTER TABLE {DIM}.dim_customer SET ROW FILTER {DIM}.demo_filter_active_customers ON (is_active)",
    "Active-customer row filter applied to dim_customer"
)

# Apply regional filter to dim_store (region column lives here; cascades through joins)
run_sql(
    f"ALTER TABLE {DIM}.dim_store SET ROW FILTER {DIM}.demo_filter_store_region ON (region)",
    "Store-region row filter applied to dim_store"
)

# COMMAND ----------

# MAGIC %md ### C3 — Verify Row Filters

# COMMAND ----------

# Count with and without the filter effect
total_customers = spark.sql(f"SELECT COUNT(*) FROM {DIM}.dim_customer").collect()[0][0]
active_count    = spark.sql(f"SELECT COUNT(*) FROM {DIM}.dim_customer WHERE is_active = TRUE").collect()[0][0]

# Note: after the row filter is applied, the first query already returns only active rows
# (unless you are an account_admin)
print(f"Rows visible from dim_customer        : {total_customers}")
print(f"  (Active-only count for reference)   : {active_count}")
print()

is_admin = spark.sql("SELECT is_account_group_member('account_admins')").collect()[0][0]
print(f"Is account_admin? : {is_admin}")
print("Expected          :", "ALL rows (including inactive)" if is_admin else "Active rows only")

# COMMAND ----------

# SQL: show row counts by region via dim_store (filter on dim_store cascades through the join)
print("=== Order counts by region (depends on group membership via dim_store row filter) ===")
spark.sql(f"""
    SELECT s.region, COUNT(*) AS order_count
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_store s ON o.store_key = s.store_key
    GROUP BY s.region
    ORDER BY order_count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Demo talking point:** Run the same query as two different users (one `global_analysts`,
# MAGIC > one `west_analysts`) and show that they see different row counts — same SQL, different data.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section D — Column Tagging
# MAGIC
# MAGIC Tags are **key-value metadata** applied to catalog/schema/table/column objects.
# MAGIC For ABAC policies (Section E), column tags are the *selector* — a policy applies
# MAGIC only to columns matching `has_tag()` or `has_tag_value()` conditions.

# COMMAND ----------

# MAGIC %md ### D1 — Apply Column Tags

# COMMAND ----------

# Tag PII columns on dim_customer
pii_column_tags = [
    (f"{DIM}.dim_customer", "email",          "pii",         "email"),
    (f"{DIM}.dim_customer", "phone",          "pii",         "phone"),
    (f"{DIM}.dim_customer", "date_of_birth",  "pii",         "dob"),
    (f"{DIM}.dim_customer", "city",           "pii",         "location"),
    (f"{DIM}.dim_customer", "postcode",       "pii",         "location"),
    (f"{DIM}.dim_employee", "email",          "pii",         "email"),
    (f"{DIM}.dim_employee", "salary_band",    "sensitivity", "financial"),
]

for table, column, tag_key, tag_val in pii_column_tags:
    run_sql(
        f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ('{tag_key}' = '{tag_val}')",
        f"Tagged {table}.{column} → {tag_key}={tag_val}"
    )

# Tag financial columns on facts
financial_column_tags = [
    (f"{FACT}.fact_order_items", "unit_cost",   "sensitivity", "financial"),
    (f"{FACT}.fact_order_items", "unit_price",  "sensitivity", "financial"),
    (f"{DIM}.dim_employee",      "salary_band", "sensitivity", "financial"),
]

for table, column, tag_key, tag_val in financial_column_tags:
    run_sql(
        f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ('{tag_key}' = '{tag_val}')",
        f"Tagged {table}.{column} → {tag_key}={tag_val}"
    )

# COMMAND ----------

# MAGIC %md ### D2 — Inspect Column Tags

# COMMAND ----------

# SQL: view all PII-tagged columns via information_schema
spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.column_tags
    WHERE tag_name IN ('pii', 'sensitivity')
    ORDER BY table_name, column_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section E — ABAC Policies (Tag-Driven, Enterprise Scale)
# MAGIC
# MAGIC ABAC policies **automatically apply** masks and filters to every column or table
# MAGIC that matches a tag condition — including tables that haven't been created yet.
# MAGIC
# MAGIC > **Requires:** Account Admin or Metastore Admin + DBR 16.4+ or Serverless compute.

# COMMAND ----------

# MAGIC %md ### E1 — Create Simple Mask UDF for ABAC (stateless, no group check)

# COMMAND ----------

# ABAC mask UDFs are intentionally simple — the EXCEPT clause in the policy
# controls who gets the mask. The UDF just defines the transformation.

run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.abac_mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'ABAC policy mask: replaces email with a***@domain format.
         Policy TO/EXCEPT clause controls which groups see this mask.'
RETURN
  CASE
    WHEN email IS NULL THEN NULL
    ELSE CONCAT(LEFT(email, 1), '***@', split_part(email, '@', 2))
  END
""", "ABAC email mask UDF (stateless) created")

run_sql(f"""
CREATE OR REPLACE FUNCTION {DIM}.abac_active_customer_filter(is_active BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT 'ABAC policy row filter: returns TRUE only for active customers.
         Policy TO/EXCEPT clause controls which principals see this filter.'
RETURN is_active = TRUE
""", "ABAC active-customer filter UDF (stateless) created")

# COMMAND ----------

# MAGIC %md ### E2 — Create ABAC Policies

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- ABAC Policy syntax (requires Account Admin + DBR 16.4+ or Serverless):
# MAGIC
# MAGIC CREATE [OR REPLACE] POLICY <policy_name>
# MAGIC ON { CATALOG <catalog> | SCHEMA <schema> | TABLE <table> }
# MAGIC COMMENT '<description>'
# MAGIC { ROW FILTER <udf_name>
# MAGIC   | COLUMN MASK <udf_name> ON COLUMN <target_column_alias> }
# MAGIC TO `<principal>` [, `<principal>`, ...]
# MAGIC [EXCEPT `<principal>` [, `<principal>`, ...]]
# MAGIC FOR TABLES
# MAGIC [MATCH COLUMNS { has_tag('<key>') | has_tag_value('<key>', '<value>') } AS <alias>
# MAGIC [, ...]]
# MAGIC [USING COLUMNS (<alias> [, <alias>, ...])];
# MAGIC ```

# COMMAND ----------

# Catalog-level policy: mask ALL pii=email columns for non-pii_readers
# This applies to every table in the catalog — present AND future
run_sql(f"""
CREATE OR REPLACE POLICY abac_mask_pii_emails
ON CATALOG {CATALOG}
COMMENT 'ABAC Demo: auto-mask all email columns tagged pii=email.
         Applies to all tables in the catalog. Exempt: pii_readers, account_admins.'
COLUMN MASK {DIM}.abac_mask_email
TO `all_users`
EXCEPT `pii_readers`, `account_admins`
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'email') AS email
ON COLUMN email
""", "ABAC catalog-level email mask policy created")

# Schema-level row filter policy: hide inactive customers across dimension tables
run_sql(f"""
CREATE OR REPLACE POLICY abac_hide_inactive_customers
ON SCHEMA {DIM}
COMMENT 'ABAC Demo: hide inactive customers from all dimension tables.
         Applies to any column tagged customer_status=active_flag in this schema.'
ROW FILTER {DIM}.abac_active_customer_filter
TO `all_users`
EXCEPT `account_admins`
FOR TABLES
MATCH COLUMNS has_tag('customer_status') AS is_active
USING COLUMNS (is_active)
""", "ABAC schema-level active-customer row filter policy created")

# COMMAND ----------

# MAGIC %md ### E3 — Demonstrate ABAC Auto-Apply to a New Table

# COMMAND ----------

# MAGIC %md
# MAGIC The power of ABAC is that new tables automatically inherit policies.
# MAGIC Create a new table with a tagged email column — no ALTER TABLE needed.

# COMMAND ----------

# Create a new contacts table with a tagged email column
run_sql(f"""
CREATE OR REPLACE TABLE {DIM}.demo_contacts (
  contact_id   INT     COMMENT 'Primary key',
  full_name    STRING  COMMENT 'Contact full name',
  work_email   STRING  COMMENT 'Work email address — tagged as PII' TAGS ('pii' = 'email'),
  department   STRING  COMMENT 'Business department'
)
COMMENT 'ABAC Demo: new table — email mask is inherited automatically from the catalog policy.'
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY employee_id) AS contact_id,
  CONCAT(first_name, ' ', last_name)       AS full_name,
  email                                    AS work_email,
  department
FROM {DIM}.dim_employee
LIMIT 20
""", "demo_contacts table created with tagged email column")

# COMMAND ----------

# Query the new table — the mask applies automatically via the ABAC policy
# No ALTER TABLE SET MASK was needed!
print("=== demo_contacts.work_email (ABAC policy auto-applied) ===")
spark.sql(f"""
    SELECT contact_id, full_name, work_email, department
    FROM {DIM}.demo_contacts
    ORDER BY contact_id
""").display()

# COMMAND ----------

is_pii = spark.sql("SELECT is_account_group_member('pii_readers')").collect()[0][0]
print("Expected:", "Full email (pii_readers member)" if is_pii else "Masked email — auto-applied via ABAC policy, no ALTER TABLE needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section F — Audit & Introspection
# MAGIC
# MAGIC Unity Catalog `information_schema` views expose all applied masks, filters, and tags.
# MAGIC Use these for governance reporting and compliance audits.

# COMMAND ----------

# MAGIC %md ### F1 — Column Masks Applied to This Catalog

# COMMAND ----------

# SQL: list all active column masks
spark.sql(f"""
    SELECT
        table_schema,
        table_name,
        column_name,
        mask_name,
        mask_schema_name
    FROM {CATALOG}.information_schema.column_masks
    ORDER BY table_schema, table_name, column_name
""").display()

# COMMAND ----------

# MAGIC %md ### F2 — Row Filters Applied to This Catalog

# COMMAND ----------

spark.sql(f"""
    SELECT
        table_schema,
        table_name,
        filter_name,
        filter_schema_name
    FROM {CATALOG}.information_schema.row_filters
    ORDER BY table_schema, table_name
""").display()

# COMMAND ----------

# MAGIC %md ### F3 — PII and Sensitive Column Tags

# COMMAND ----------

spark.sql(f"""
    SELECT
        table_schema,
        table_name,
        column_name,
        tag_name,
        tag_value
    FROM {CATALOG}.information_schema.column_tags
    WHERE tag_name IN ('pii', 'sensitivity', 'customer_status')
    ORDER BY table_name, column_name
""").display()

# COMMAND ----------

# MAGIC %md ### F4 — Table-Level Tags (governance overview)

# COMMAND ----------

spark.sql(f"""
    SELECT
        table_schema,
        table_name,
        tag_name,
        tag_value
    FROM {CATALOG}.information_schema.table_tags
    ORDER BY table_schema, table_name, tag_name
""").display()

# COMMAND ----------

# MAGIC %md ### F5 — Governance Summary Dashboard

# COMMAND ----------

# Python: build a concise governance health summary
masks_df   = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.information_schema.column_masks")
filters_df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.information_schema.row_filters")
col_tags   = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.information_schema.column_tags WHERE tag_name = 'pii'")
tbl_tags   = spark.sql(f"SELECT COUNT(DISTINCT table_name) AS cnt FROM {CATALOG}.information_schema.table_tags")

mask_count   = masks_df.collect()[0][0]
filter_count = filters_df.collect()[0][0]
pii_count    = col_tags.collect()[0][0]
tagged_tbls  = tbl_tags.collect()[0][0]

print("=" * 50)
print(f"  Governance Summary for catalog: {CATALOG}")
print("=" * 50)
print(f"  Column masks applied      : {mask_count}")
print(f"  Row filters applied       : {filter_count}")
print(f"  PII-tagged columns        : {pii_count}")
print(f"  Tables with tags          : {tagged_tbls}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section G — Live Demo Scenarios
# MAGIC
# MAGIC Use these cells as talking points during a live demo.
# MAGIC Run the same query and show how the output changes based on group membership.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 — PII Masking (Same Query, Different Results)
# MAGIC
# MAGIC **Script:**
# MAGIC > *"Both analysts are running the exact same SELECT. The data they see is different
# MAGIC > because Unity Catalog applies the column mask transparently — no application code change needed."*

# COMMAND ----------

# SQL shown to the audience (both users run this)
demo_query = f"""
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    loyalty_tier,
    region
FROM {DIM}.dim_customer
WHERE loyalty_tier = 'Platinum'
ORDER BY customer_id
LIMIT 10
"""

print("Running demo query as:", spark.sql("SELECT current_user()").collect()[0][0])
spark.sql(demo_query).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2 — Financial Data (Margin Calculation Blocked)
# MAGIC
# MAGIC **Script:**
# MAGIC > *"The `unit_cost` column is masked to NULL for non-finance users.
# MAGIC > This means any downstream derived metric — like gross margin — also becomes NULL.
# MAGIC > You can't reverse-engineer cost data by combining other columns."*

# COMMAND ----------

spark.sql(f"""
SELECT
    product_id,
    SUM(quantity)                                     AS units_sold,
    ROUND(SUM(unit_price * quantity), 2)              AS gross_revenue,
    ROUND(SUM(unit_cost  * quantity), 2)              AS total_cogs,
    ROUND(SUM((unit_price - unit_cost) * quantity), 2) AS gross_profit
FROM {FACT}.fact_order_items
GROUP BY product_id
ORDER BY gross_revenue DESC
LIMIT 10
""").display()

# COMMAND ----------

is_finance = spark.sql("SELECT is_account_group_member('finance_role')").collect()[0][0]
print(f"Current user in finance_role? : {is_finance}")
print("Expected total_cogs / gross_profit:", "Populated" if is_finance else "NULL (unit_cost masked)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3 — Regional Row Filter
# MAGIC
# MAGIC **Script:**
# MAGIC > *"This analyst is a member of `west_analysts`. The row filter on `dim_store`
# MAGIC > restricts which stores are visible. When joined to `fact_orders`, only West region
# MAGIC > orders appear. The filter is invisible to the query — it's enforced at the Unity Catalog layer."*

# COMMAND ----------

spark.sql(f"""
SELECT
    s.region,
    o.channel,
    COUNT(*)                          AS order_count,
    ROUND(SUM(o.total_amount), 2)     AS total_revenue
FROM {FACT}.fact_orders o
JOIN {DIM}.dim_store s ON o.store_key = s.store_key
GROUP BY s.region, o.channel
ORDER BY s.region, total_revenue DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 4 — ABAC Scale (Zero-Touch Governance)
# MAGIC
# MAGIC **Script:**
# MAGIC > *"This is the enterprise differentiator. We just created a new table with an email
# MAGIC > column tagged `pii=email`. The catalog-level ABAC policy automatically masked it —
# MAGIC > the data engineer who created the table didn't need to write any ALTER TABLE statement."*

# COMMAND ----------

# Prove the new demo_contacts table is protected automatically
print("=== New table (demo_contacts) — email auto-masked by ABAC policy ===")
spark.sql(f"SELECT contact_id, full_name, work_email FROM {DIM}.demo_contacts").display()

# Show the tag that triggered automatic protection
print("\n=== Tag that triggered the ABAC policy ===")
spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.column_tags
    WHERE table_name = 'demo_contacts'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section H — Cleanup (Optional)
# MAGIC
# MAGIC Run this cell to remove the demo-specific masks and filters created in this notebook.
# MAGIC The masks created by `06_governance.py` (production governance) are **not** removed.

# COMMAND ----------

CLEANUP = False  # Set to True to remove demo objects

if CLEANUP:
    cleanup_steps = [
        # Remove masks and filters from tables first (required before dropping UDFs)
        (f"ALTER TABLE {DIM}.dim_customer ALTER COLUMN email DROP MASK",            "Dropped email mask on dim_customer"),
        (f"ALTER TABLE {DIM}.dim_customer ALTER COLUMN phone DROP MASK",            "Dropped phone mask on dim_customer"),
        (f"ALTER TABLE {DIM}.dim_employee ALTER COLUMN salary_band DROP MASK",      "Dropped salary_band mask on dim_employee"),
        (f"ALTER TABLE {FACT}.fact_order_items ALTER COLUMN unit_cost DROP MASK",   "Dropped unit_cost mask on fact_order_items"),
        (f"ALTER TABLE {DIM}.dim_customer DROP ROW FILTER",                         "Dropped row filter on dim_customer"),
        (f"ALTER TABLE {DIM}.dim_store DROP ROW FILTER",                             "Dropped row filter on dim_store"),
        # Drop ABAC policies
        (f"DROP POLICY IF EXISTS abac_mask_pii_emails ON CATALOG {CATALOG}",        "Dropped ABAC email mask policy"),
        (f"DROP POLICY IF EXISTS abac_hide_inactive_customers ON SCHEMA {DIM}",     "Dropped ABAC row filter policy"),
        # Drop demo UDFs
        (f"DROP FUNCTION IF EXISTS {DIM}.demo_mask_email",                          "Dropped demo_mask_email UDF"),
        (f"DROP FUNCTION IF EXISTS {DIM}.demo_mask_phone",                          "Dropped demo_mask_phone UDF"),
        (f"DROP FUNCTION IF EXISTS {DIM}.demo_mask_salary_band",                    "Dropped demo_mask_salary_band UDF"),
        (f"DROP FUNCTION IF EXISTS {FACT}.demo_mask_unit_cost",                     "Dropped demo_mask_unit_cost UDF"),
        (f"DROP FUNCTION IF EXISTS {DIM}.demo_filter_active_customers",             "Dropped demo_filter_active_customers UDF"),
        (f"DROP FUNCTION IF EXISTS {DIM}.demo_filter_store_region",                 "Dropped demo_filter_store_region UDF"),
        (f"DROP FUNCTION IF EXISTS {DIM}.abac_mask_email",                          "Dropped abac_mask_email UDF"),
        (f"DROP FUNCTION IF EXISTS {DIM}.abac_active_customer_filter",              "Dropped abac_active_customer_filter UDF"),
        # Drop demo table
        (f"DROP TABLE IF EXISTS {DIM}.demo_contacts",                               "Dropped demo_contacts table"),
    ]
    for sql, label in cleanup_steps:
        run_sql(sql, label)
    print("\nCleanup complete.")
else:
    print("CLEANUP = False — no objects removed.")
    print("Set CLEANUP = True in this cell to remove demo objects.")
