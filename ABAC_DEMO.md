# Databricks Attribute-Based Access Control (ABAC) — Demo Guide

> **Companion to the Northwind Analytics sample database.**
> Run notebook `10_abac_demo.py` to execute every example interactively.

---

## Contents

1. [What Is ABAC?](#1-what-is-abac)
2. [Two Approaches: Manual vs Policy-Driven](#2-two-approaches-manual-vs-policy-driven)
3. [Key Identity Functions](#3-key-identity-functions)
4. [Row Filters](#4-row-filters)
5. [Column Masks (Dynamic Data Masking)](#5-column-masks-dynamic-data-masking)
6. [Tag-Driven ABAC Policies](#6-tag-driven-abac-policies)
7. [Demo Scenarios (Northwind Analytics)](#7-demo-scenarios-northwind-analytics)
8. [Auditing & Introspection](#8-auditing--introspection)
9. [Permissions Required](#9-permissions-required)
10. [Limitations](#10-limitations)
11. [Documentation References](#11-documentation-references)

---

## 1. What Is ABAC?

**Attribute-Based Access Control (ABAC)** in Databricks Unity Catalog lets you control access to **rows and columns** based on *attributes* — properties of the user (group membership, current identity) or of the data itself (column tags, table tags).

Unlike Role-Based Access Control (RBAC), which grants blanket `SELECT` on a table, ABAC allows the same `SELECT` to return *different data* depending on who is asking.

```
┌──────────────┐    SELECT * FROM dim_customer
│   Analyst A  │──────────────────────────────► Full email shown
│  (pii_readers│                                (500 rows, all regions)
│   group)     │
└──────────────┘

┌──────────────┐    SELECT * FROM dim_customer
│   Analyst B  │──────────────────────────────► Email masked as a***@gmail.com
│  (no group)  │                                (500 rows, home region only)
└──────────────┘
```

### Components

| Component | Role |
|-----------|------|
| **Row Filter** | SQL UDF returning `BOOLEAN` — rows where it returns `FALSE` are hidden |
| **Column Mask** | SQL UDF returning the (optionally masked) column value |
| **Tag** | Key-value metadata on catalog/schema/table/column objects |
| **ABAC Policy** | Centralized policy that binds a mask/filter to objects *by tag*, not by name |

---

## 2. Two Approaches: Manual vs Policy-Driven

| | **Manual (Per-Table)** | **ABAC Policies (Tag-Driven)** |
|---|---|---|
| **How it works** | `ALTER TABLE … SET ROW FILTER` / `ALTER COLUMN … SET MASK` | `CREATE POLICY … FOR TABLES MATCH COLUMNS has_tag(…)` |
| **Scope** | One table at a time | Entire catalog or schema, including *future* tables |
| **Admin level** | Table owner / data engineer | Account Admin or Metastore Admin |
| **Compute** | DBR 12.2 LTS+ | DBR 16.4+ or Serverless |
| **Best for** | Targeted, known tables | Enterprise-scale, governance-as-code |

> **Northwind demo uses manual approach** (compatible with all runtimes).
> The ABAC policy section shows the policy-driven approach for completeness.

---

## 3. Key Identity Functions

These SQL built-ins are the building blocks of every filter and mask:

### `current_user()` / `session_user()`

Returns the email (or UUID for service principals) of the session user.

```sql
SELECT current_user();
-- → 'james@company.com'
```

### `is_account_group_member(group_name)`

Returns `TRUE` if the user is a **direct or indirect** member of an **account-level** Unity Catalog group.
**Prefer this over `is_member()` for Unity Catalog objects.**

```sql
SELECT is_account_group_member('pii_readers');
-- → true / false
```

### `is_member(group_name)`

Returns `TRUE` for **workspace-local** groups OR account-level groups assigned to the workspace.
Compatible with Hive Metastore and older runtimes. Used in this repo's existing `06_governance.py`.

```sql
SELECT is_member('analysts');
```

> **Which to use?**
> Use `is_account_group_member()` for new Unity Catalog policies.
> Use `is_member()` only when you need workspace-local group support or backward compatibility.

---

## 4. Row Filters

A row filter is a **SQL UDF returning `BOOLEAN`**. Rows where it returns `FALSE` or `NULL` are silently excluded from query results.

### SQL — Create a Row Filter

```sql
-- Filter: analysts only see customers in their assigned region
CREATE OR REPLACE FUNCTION northwind.dimensions.region_row_filter(region STRING)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT 'Restricts dim_customer rows by store region based on group membership.
         Global analysts see all regions; regional analysts see their region only.'
RETURN
  CASE
    WHEN is_account_group_member('account_admins')  THEN TRUE  -- admins bypass
    WHEN is_account_group_member('global_analysts') THEN TRUE  -- global team sees all
    WHEN is_account_group_member('west_analysts')   THEN region = 'West'
    WHEN is_account_group_member('east_analysts')   THEN region = 'East'
    WHEN is_account_group_member('central_analysts') THEN region = 'Central'
    ELSE FALSE  -- deny by default if no matching group
  END;
```

### SQL — Apply to a Table

```sql
-- Apply to an existing table (dim_store has the region column)
ALTER TABLE northwind.dimensions.dim_store
SET ROW FILTER northwind.dimensions.region_row_filter ON (region);

-- Apply at CREATE time
CREATE TABLE northwind.dimensions.dim_store (
  store_id   INT,
  region     STRING,
  ...
)
WITH ROW FILTER northwind.dimensions.region_row_filter ON (region);

-- Remove the filter
ALTER TABLE northwind.dimensions.dim_store DROP ROW FILTER;
```

### Python — Create and Apply a Row Filter

```python
catalog = "northwind"
schema  = "dimensions"
table   = f"{catalog}.{schema}.dim_store"

# Create the UDF
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.region_row_filter(region STRING)
    RETURNS BOOLEAN
    LANGUAGE SQL
    COMMENT 'Row-level security: restrict stores by region'
    RETURN
      CASE
        WHEN is_account_group_member('account_admins')  THEN TRUE
        WHEN is_account_group_member('global_analysts') THEN TRUE
        WHEN is_account_group_member('west_analysts')   THEN region = 'West'
        WHEN is_account_group_member('east_analysts')   THEN region = 'East'
        ELSE FALSE
      END
""")

# Apply to the table
spark.sql(f"""
    ALTER TABLE {table}
    SET ROW FILTER {catalog}.{schema}.region_row_filter ON (region)
""")

print(f"Row filter applied to {table}")
```

### Verify Row Filter Is Working

```python
# The count should differ based on current user's group membership
count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
user  = spark.sql("SELECT current_user()").collect()[0][0]
print(f"User '{user}' sees {count} rows in dim_customer")
```

---

## 5. Column Masks (Dynamic Data Masking)

A column mask is a **SQL UDF** whose return value replaces the raw column value in query results.
The first UDF parameter maps 1:1 to the masked column. Additional context columns can be passed via `USING COLUMNS`.

### SQL — Create Column Masks

```sql
-- Email mask: show only first char + ***@domain to non-PII readers
CREATE OR REPLACE FUNCTION northwind.dimensions.mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'PII masking: non pii_readers see a***@domain.com format'
RETURN
  CASE
    WHEN is_account_group_member('pii_readers')    THEN email  -- full email
    WHEN is_account_group_member('account_admins') THEN email  -- admins bypass
    WHEN email IS NULL                             THEN NULL
    ELSE CONCAT(LEFT(email, 1), '***@', split_part(email, '@', 2))
  END;

-- Phone mask: last 4 digits only
CREATE OR REPLACE FUNCTION northwind.dimensions.mask_phone(phone STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'PII masking: non pii_readers see ***-***-XXXX (last 4 only)'
RETURN
  CASE
    WHEN is_account_group_member('pii_readers')    THEN phone
    WHEN is_account_group_member('account_admins') THEN phone
    WHEN phone IS NULL                             THEN NULL
    ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4))
  END;

-- Salary band mask: HR only
CREATE OR REPLACE FUNCTION northwind.dimensions.mask_salary_band(salary_band STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Salary bands are visible only to hr_role group members'
RETURN
  CASE
    WHEN is_account_group_member('hr_role')        THEN salary_band
    WHEN is_account_group_member('account_admins') THEN salary_band
    ELSE 'REDACTED'
  END;

-- Unit cost mask with context: finance only
CREATE OR REPLACE FUNCTION northwind.facts.mask_unit_cost(unit_cost DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Cost data visible only to finance_role group members. Others see NULL.'
RETURN
  CASE
    WHEN is_account_group_member('finance_role')   THEN unit_cost
    WHEN is_account_group_member('account_admins') THEN unit_cost
    ELSE NULL
  END;
```

### SQL — Apply Column Masks to Tables

```sql
-- Apply masks to PII columns on dim_customer
ALTER TABLE northwind.dimensions.dim_customer
ALTER COLUMN email SET MASK northwind.dimensions.mask_email;

ALTER TABLE northwind.dimensions.dim_customer
ALTER COLUMN phone SET MASK northwind.dimensions.mask_phone;

-- Apply cost mask on fact table
ALTER TABLE northwind.facts.fact_order_items
ALTER COLUMN unit_cost SET MASK northwind.facts.mask_unit_cost;

-- Remove a mask
ALTER TABLE northwind.dimensions.dim_customer
ALTER COLUMN email DROP MASK;
```

### Python — Bulk Apply Masks

```python
catalog  = "northwind"
dim_tbl  = f"{catalog}.dimensions.dim_customer"
fact_tbl = f"{catalog}.facts.fact_order_items"

# PII masks on customer table
pii_masks = {
    "email": f"{catalog}.dimensions.mask_email",
    "phone": f"{catalog}.dimensions.mask_phone",
}
for col, fn in pii_masks.items():
    spark.sql(f"ALTER TABLE {dim_tbl} ALTER COLUMN {col} SET MASK {fn}")
    print(f"  Applied mask on {dim_tbl}.{col}")

# Cost mask on fact table
spark.sql(f"""
    ALTER TABLE {fact_tbl}
    ALTER COLUMN unit_cost SET MASK {catalog}.facts.mask_unit_cost
""")
print(f"  Applied cost mask on {fact_tbl}.unit_cost")
```

### Verify Masking

```sql
-- As a non-pii_readers user:
SELECT customer_id, email, phone FROM northwind.dimensions.dim_customer LIMIT 5;
-- Expected: a***@gmail.com   ***-***-4821

-- After adding yourself to pii_readers:
-- Expected: actual_email@gmail.com   555-123-4821
```

---

## 6. Tag-Driven ABAC Policies

ABAC policies are centralized rules applied to **all current and future tables** matching a tag condition. This is the enterprise-scale approach.

> **Requires:** Account Admin or Metastore Admin role + DBR 16.4+ or Serverless compute.

### Step 1 — Create Governed Tags

```sql
-- Governed tags must be defined at account level (UI or SQL)
-- Then apply them to objects:

-- Schema-level tag
ALTER SCHEMA northwind.dimensions
SET TAGS ('domain' = 'customers', 'data_classification' = 'sensitive');

-- Column-level tags (the key input for ABAC MATCH COLUMNS)
ALTER TABLE northwind.dimensions.dim_customer
ALTER COLUMN email SET TAGS ('pii' = 'email');

ALTER TABLE northwind.dimensions.dim_customer
ALTER COLUMN phone SET TAGS ('pii' = 'phone');

ALTER TABLE northwind.facts.fact_order_items
ALTER COLUMN unit_cost SET TAGS ('sensitivity' = 'financial');
```

### Step 2 — Create Policy UDFs

```sql
-- Mask UDF (simple — the TO/EXCEPT clause in the policy controls who it applies to)
CREATE OR REPLACE FUNCTION northwind.dimensions.abac_mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
RETURN CONCAT(LEFT(email, 1), '***@', split_part(email, '@', 2));

-- Row filter UDF
CREATE OR REPLACE FUNCTION northwind.dimensions.abac_active_filter(is_active BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
RETURN is_active = TRUE;
```

### Step 3 — Create ABAC Policies

```sql
-- Column mask policy: applies to ALL columns tagged pii=email across the entire catalog
CREATE OR REPLACE POLICY mask_pii_emails
ON CATALOG northwind
COMMENT 'Auto-mask all email columns tagged pii=email for non-pii_readers'
COLUMN MASK northwind.dimensions.abac_mask_email
TO `all_users`
EXCEPT `pii_readers`, `account_admins`
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'email') AS email
ON COLUMN email;

-- Row filter policy: hide inactive customers across all tables with is_active column
CREATE OR REPLACE POLICY hide_inactive_customers
ON SCHEMA northwind.dimensions
COMMENT 'Filter out inactive customers from all dimension tables'
ROW FILTER northwind.dimensions.abac_active_filter
TO `all_users`
FOR TABLES
MATCH COLUMNS has_tag('customer_status') AS is_active
USING COLUMNS (is_active);

-- Drop a policy
DROP POLICY mask_pii_emails ON CATALOG northwind;
```

### Python — Full ABAC Tag + Policy Setup

```python
catalog = "northwind"

# Step 1: Tag the columns
tag_specs = [
    (f"{catalog}.dimensions.dim_customer", "email",     "pii",         "email"),
    (f"{catalog}.dimensions.dim_customer", "phone",     "pii",         "phone"),
    (f"{catalog}.facts.fact_order_items",  "unit_cost", "sensitivity", "financial"),
]

for table, column, tag_key, tag_val in tag_specs:
    spark.sql(f"""
        ALTER TABLE {table}
        ALTER COLUMN `{column}`
        SET TAGS ('{tag_key}' = '{tag_val}')
    """)
    print(f"Tagged {table}.{column} → {tag_key}={tag_val}")

# Step 2: Create ABAC policy
spark.sql(f"""
    CREATE OR REPLACE POLICY mask_pii_emails
    ON CATALOG {catalog}
    COMMENT 'Auto-mask email columns tagged pii=email for non-pii_readers'
    COLUMN MASK {catalog}.dimensions.abac_mask_email
    TO `all_users`
    EXCEPT `pii_readers`, `account_admins`
    FOR TABLES
    MATCH COLUMNS has_tag_value('pii', 'email') AS email
    ON COLUMN email
""")

print("ABAC policy created — any column tagged pii=email is now automatically masked")
```

---

## 7. Demo Scenarios (Northwind Analytics)

### Scenario A — PII Masking Demo

**Setup:** Two users (or sessions) — one in `pii_readers` group, one not.

```sql
-- Both users run this query:
SELECT customer_id, first_name, email, phone
FROM northwind.dimensions.dim_customer
LIMIT 10;

-- pii_readers user sees:
--   1001 | Alice | alice@gmail.com      | 555-234-1234
--   1002 | Bob   | bob@company.com      | 555-876-5432

-- Regular analyst sees:
--   1001 | Alice | a***@gmail.com       | ***-***-1234
--   1002 | Bob   | b***@company.com     | ***-***-5432
```

### Scenario B — Regional Row Filter Demo

**Setup:** `west_analysts` group member vs `global_analysts` member.

```sql
-- Both users run:
SELECT region, COUNT(*) AS stores
FROM northwind.dimensions.dim_store
GROUP BY region
ORDER BY stores DESC;

-- global_analysts sees all 5 regions
-- west_analysts sees only 'West' rows
```

### Scenario C — Financial Data Access

**Setup:** `finance_role` group vs regular analyst.

```sql
-- Both users run:
SELECT product_id, quantity, unit_price, unit_cost,
       (unit_price - unit_cost) AS gross_margin
FROM northwind.facts.fact_order_items
LIMIT 5;

-- finance_role user: unit_cost = 12.50, gross_margin = 7.50
-- Regular analyst:   unit_cost = NULL,  gross_margin = NULL
```

### Scenario D — ABAC Scale Demo (Enterprise)

**Talking point:** Show that adding a new table with a `pii=email` tag instantly inherits the masking policy — no manual `ALTER TABLE` required.

```sql
-- Create a new table with a tagged email column
CREATE TABLE northwind.dimensions.dim_employee_v2 (
  employee_id INT,
  name        STRING,
  work_email  STRING TAGS ('pii' = 'email'),
  department  STRING
) AS SELECT * FROM northwind.dimensions.dim_employee;

-- The ABAC policy automatically masks work_email for non-pii_readers
-- No ALTER TABLE needed!
SELECT employee_id, work_email FROM northwind.dimensions.dim_employee_v2 LIMIT 5;
```

### Scenario E — Introspection (Governance Audit)

```sql
-- See all column masks in the catalog
SELECT table_schema, table_name, column_name, mask_name
FROM northwind.information_schema.column_masks
ORDER BY table_schema, table_name;

-- See all row filters
SELECT table_schema, table_name, filter_name
FROM northwind.information_schema.row_filters
ORDER BY table_schema;

-- See all column tags (shows what ABAC policies will match)
SELECT table_name, column_name, tag_name, tag_value
FROM northwind.information_schema.column_tags
WHERE tag_name = 'pii'
ORDER BY table_name;
```

---

## 8. Auditing & Introspection

### Information Schema Views

```sql
-- Column masks applied across all tables in a catalog
SELECT * FROM <catalog>.information_schema.column_masks;

-- Row filters applied across all tables
SELECT * FROM <catalog>.information_schema.row_filters;

-- Tags on columns (key input for ABAC policy matching)
SELECT * FROM <catalog>.information_schema.column_tags;

-- Tags on tables
SELECT * FROM <catalog>.information_schema.table_tags;

-- Tags on schemas
SELECT * FROM <catalog>.information_schema.schema_tags;
```

### SHOW Commands

```sql
-- Inspect a row filter UDF
DESCRIBE FUNCTION EXTENDED northwind.dimensions.region_row_filter;

-- Check table properties (tags, filters, masks visible in TBLPROPERTIES)
DESCRIBE EXTENDED northwind.dimensions.dim_customer;

-- List all functions in a schema
SHOW FUNCTIONS IN northwind.dimensions;
```

### Python Audit Script

```python
catalog = "northwind"

print("=== Column Masks ===")
display(spark.sql(f"""
    SELECT table_schema, table_name, column_name, mask_name
    FROM {catalog}.information_schema.column_masks
    ORDER BY table_schema, table_name, column_name
"""))

print("=== Row Filters ===")
display(spark.sql(f"""
    SELECT table_schema, table_name, filter_name
    FROM {catalog}.information_schema.row_filters
    ORDER BY table_schema, table_name
"""))

print("=== PII-Tagged Columns ===")
display(spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {catalog}.information_schema.column_tags
    WHERE tag_name = 'pii'
    ORDER BY table_name, column_name
"""))
```

---

## 9. Permissions Required

| Action | Required Privilege |
|--------|-------------------|
| Create a row filter / column mask UDF | `CREATE FUNCTION` on the schema |
| Apply a row filter to a table | `EXECUTE` on the UDF + `MANAGE` or `OWNERSHIP` on the table |
| Apply a column mask to a table | Same as row filter |
| Drop a UDF | Must remove all masks/filters referencing it first |
| Create an ABAC policy | **Account Admin** or **Metastore Admin** |
| Assign governed tags | Tag admin rights (configurable per tag in Account Console) |
| View `information_schema` governance views | `USE CATALOG` + `USE SCHEMA` on target |

---

## 10. Limitations

| Limitation | Detail |
|-----------|--------|
| Views | Masks and filters apply to **base tables only**; views inherit security from the underlying table |
| One filter per table | A table can have **only one** row filter function |
| One mask per column | Each column can have **at most one** active column mask |
| No time travel | `SELECT * FROM table VERSION AS OF` is blocked on masked/filtered tables |
| No `CREATE TABLE CLONE` | Cloning is not supported on filtered/masked tables |
| No Delta Sharing | Cannot use Delta Sharing providers on masked/filtered tables |
| No path-based access | Accessing a table via its file path (e.g. `dbfs:/`) bypasses row/column security |
| No circular dependencies | A filter UDF cannot reference a table that itself has a filter |
| ABAC compute requirement | ABAC policies require **DBR 16.4+** or Serverless (manual approach: DBR 12.2+) |
| ABAC MATCH COLUMNS | Maximum **3 column conditions** per `MATCH COLUMNS` clause |
| Vector Search | Not supported on ABAC-protected tables |
| VARIANT type | Supported for column masks but limited for row filters |

---

## 11. Documentation References

| Topic | URL |
|-------|-----|
| ABAC Overview | `https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/` |
| Create & Manage ABAC Policies | `https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies` |
| ABAC Tutorial | `https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/tutorial` |
| Row Filters & Column Masks (Overview) | `https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/` |
| Manually Apply Filters & Masks | `https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply` |
| ROW FILTER SQL Reference | `https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-row-filter` |
| COLUMN MASK SQL Reference | `https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-column-mask` |
| UDF Best Practices for ABAC | `https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/udf-best-practices` |
| Governed Tags | `https://docs.databricks.com/aws/en/admin/governed-tags/` |
| Apply Tags to UC Objects | `https://docs.databricks.com/aws/en/database-objects/tags` |
| `is_account_group_member` | `https://docs.databricks.com/aws/en/sql/language-manual/functions/is_account_group_member` |
| `is_member` | `https://docs.databricks.com/aws/en/sql/language-manual/functions/is_member` |
| `current_user` | `https://docs.databricks.com/aws/en/sql/language-manual/functions/current_user` |
| Row Filters & Column Masks (Azure) | `https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/filters-and-masks/` |

---

*Notebook: `notebooks/10_abac_demo.py` — run this to execute all examples against the Northwind Analytics catalog.*
