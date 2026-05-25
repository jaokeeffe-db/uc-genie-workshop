# Northwind Analytics — Databricks Sample Database

## For use with the UC and Genie Workshop

A comprehensive sample retail database for Databricks, built to showcase **Unity Catalog**, **Genie (AI/BI)**, **Delta Lake**, and **ML Model Registry** capabilities.

## What Gets Created

| Schema | Objects | ~Rows | Description |
|--------|---------|-------|-------------|
| `dimensions` | 6 tables | 4,300 | Customer, product, store, employee, date, promotion |
| `facts` | 4 tables | 220,000 | Orders, order items, inventory snapshots, returns |
| `mart` | 6 views + 1 table + 4 metric views | varies | Genie-optimised analytics and semantic layer objects |
| `ml` | 4 tables + 1 model | 6,000 | Feature tables, churn predictions, ML model in UC registry |
| `raw` | 1 managed volume | — | File landing zone with seed CSV files |

## Unity Catalog Features Showcased

| Feature | Where Applied |
|---------|--------------|
| **Catalog, schema, table, column tags** | All objects — domain, PII, SLA, owner, GDPR tags |
| **Table and column descriptions/comments** | Every table and column — critical for Genie understanding |
| **Liquid Clustering** | All fact tables and large dimension tables |
| **Row Filters** | `dim_customer` (active filter), `fact_orders` (status filter) |
| **Column Masks** | Email, phone (PII), salary band (HR), unit cost (finance) |
| **Primary & Foreign Keys** | Full star-schema PK/FK relationships (`NOT ENFORCED RELY`) |
| **Managed Volume** | `raw.raw_uploads` with seed CSV files |
| **UC Model Registry** | `ml.churn_predictor` with `champion` alias |
| **Fine-grained Permissions** | GRANT statements for analysts, pii_readers, finance_role, hr_role |
| **Metric Views** | 4 semantic-layer metric views (`mv_sales`, `mv_customers`, `mv_products`, `mv_stores`) |

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- Cluster running **DBR 15.4 LTS+** or **Serverless SQL warehouse** (required for metric views; DBR 14.3 LTS+ is sufficient for all other notebooks)
- `CREATE CATALOG` privilege (or provide an existing catalog and have `CREATE SCHEMA`)
- All notebooks imported to the **same folder** in your Databricks workspace

## Deployment

### Option 1: Databricks CLI (recommended)

```bash
# Install CLI if needed
pip install databricks-cli

# Upload notebooks to your workspace
databricks workspace import-dir notebooks/ /Shared/northwind-analytics/ --overwrite

# Open the workspace and run 00_main from your browser
```

### Option 2: Manual import

1. In your Databricks workspace, click **Workspace** → **Import**
2. Select **"Import from file"** and upload the entire `notebooks/` folder
3. Or import each `.py` file individually — Databricks recognises the `# Databricks notebook source` header

### Option 3: Repos (Git integration)

1. In the workspace, go to **Repos** → **Add Repo**
2. Connect this repository
3. Navigate to `notebooks/00_main` and run it

### Running the Deployment

1. Open `notebooks/00_main` in your workspace
2. Configure the widgets at the top:
   - `catalog_name`: name for the new catalog (default: `sample_db`)
   - `catalog_managed_location`: optional external storage location for the catalog
   - `env`: `dev` or `prod` (affects whether GRANTs are applied)
   - `reset_catalog`: `true` to drop and recreate from scratch
   - `num_customers`: customers to generate (default: 2,000)
   - `num_orders`: orders to generate (default: 50,000)
3. Click **Run All**
4. Estimated runtime: **10–20 minutes** on a single-node cluster

`00_main` runs all 10 steps in sequence, including the supplementary Genie optimisation and metric view notebooks. The final status table shows timing and any failures.

## Notebook Structure

```
notebooks/
├── 00_main.py                        Orchestrator — runs all child notebooks in sequence
├── 01_catalog_setup.py               Creates catalog, schemas, volume; sets tags
├── 02_dimension_tables.py            Generates dim_date, dim_customer, dim_product, dim_store,
│                                     dim_employee, dim_promotion
├── 03_fact_tables.py                 Generates fact_orders, fact_order_items, fact_inventory,
│                                     fact_returns with realistic seasonality
├── 04_analytics_views.py             Creates mart schema views and agg_daily_sales table
├── 05_ml_models.py                   Creates feature tables, trains churn model, registers to UC
├── 06_governance.py                  Applies column masks, row filters, tags, comments, PKs/FKs
├── 07_validate.py                    Row count, integrity, mask, and object existence checks
│
│   — Supplementary: Genie Demo Optimisation —
│
├── 08_genie_metadata_augmentation.py Adds v_sales_current, v_kpi_executive, metric_definitions,
│                                     enhanced column comments, and Genie-optimised tags
├── 09_demo_data_refresh.py           Refreshes agg_daily_sales, updates ML timestamps,
│                                     creates v_live_kpi for trailing-30-day queries
└── 11_metric_views.py                Creates mv_sales, mv_customers, mv_products, mv_stores
                                      metric views — semantic layer for Genie AI/BI
```

## Data Model

```
                    dim_date
                       │
dim_promotion ─────────┤
                       │
dim_store ─────────────┤
                       ├──→ fact_orders ──→ fact_order_items ──→ dim_product
dim_customer ──────────┤         │
                       │         └──→ fact_returns
dim_employee ──────────┘

dim_product ──→ fact_inventory ──→ dim_store

ml.customer_features ──→ ml.churn_predictor (UC Model Registry)
                    ──→ ml.churn_predictions

mart.v_* ──→ (views over dimensions + facts)
mart.agg_daily_sales ──→ (pre-aggregated Delta table)
mart.mv_* ──→ (metric views — semantic layer for Genie)
```

## Mart Schema Objects

| Object | Type | Description |
|--------|------|-------------|
| `v_sales_summary` | View | Daily revenue by store, channel, segment, and region |
| `v_customer_360` | View | Customer LTV, recency, churn risk, loyalty tier |
| `v_product_performance` | View | Sales, returns, gross margin by product |
| `v_store_performance` | View | Store revenue vs monthly targets |
| `v_employee_sales` | View | Sales associate rankings and performance |
| `v_cohort_analysis` | View | Customer retention cohorts |
| `agg_daily_sales` | Table | Pre-aggregated daily totals for fast trend queries |
| `mv_sales` | Metric View | Revenue, orders, discount rate, AOV — semantic layer |
| `mv_customers` | Metric View | Customer health, LTV, churn risk — semantic layer |
| `mv_products` | Metric View | Product revenue, margin, return rate — semantic layer |
| `mv_stores` | Metric View | Store performance vs target, efficiency — semantic layer |
| `v_sales_current` | View | Date-shifted revenue for "this month/quarter" queries *(added by notebook 08)* |
| `v_kpi_executive` | View | Single-row YTD/QTD/MTD executive KPI summary *(added by notebook 08)* |
| `v_live_kpi` | View | Trailing-30-day KPIs computed dynamically *(added by notebook 09)* |
| `metric_definitions` | Table | Business metric glossary for Genie grounding *(added by notebook 08)* |

## Metric Views

Metric views (`mv_*`) are a Genie semantic layer built on top of the mart views and aggregate tables. Each metric view defines **dimensions** (categorical attributes for grouping/filtering) and **measures** (pre-approved KPI formulas with business-friendly names and synonyms). This gives Genie a reliable, governed vocabulary for answering natural-language questions.

| Metric View | Source | Key Measures |
|-------------|--------|--------------|
| `mv_sales` | `agg_daily_sales` | total_revenue, order_count, discount_rate, avg_order_value, return_rate |
| `mv_customers` | `v_customer_360` | customer_count, churn_risk_rate, avg_lifetime_value, avg_orders_per_customer |
| `mv_products` | `v_product_performance` | total_revenue, gross_margin_pct, avg_return_rate_pct, total_units_sold |
| `mv_stores` | `v_store_performance` | total_revenue, pct_of_target, avg_revenue_per_employee, avg_revenue_per_sqft |

> **Requirement**: Metric views require **DBR 15.4 LTS+** or a **Serverless SQL warehouse**.

## Post-Deployment: Set Up a Genie Space

1. In the Databricks left sidebar, click **Genie** → **New Genie Space**
2. Name it **"Northwind Retail Analytics"**
3. Add these tables from the `mart` schema:

| Table | Best For |
|-------|----------|
| `mv_sales` | Revenue trends, channel performance, geographic analysis (metric view) |
| `mv_customers` | Customer LTV, churn risk, segment analysis (metric view) |
| `mv_products` | Product mix, return rates, category margin (metric view) |
| `mv_stores` | Store rankings, vs-target analysis (metric view) |
| `v_sales_current` | Current-period queries ("this month", "this quarter") |
| `v_customer_360` | Detailed customer profiles and churn risk |
| `agg_daily_sales` | Fast daily/weekly/monthly revenue trends |
| `v_kpi_executive` | Single-row KPI summary for executive overview |

4. Try these starter questions:
   - *"What was total revenue last quarter by region?"*
   - *"Which 5 stores had the highest revenue in 2024?"*
   - *"Show me customers at churn risk with lifetime value over $2,000"*
   - *"What is the return rate for Electronics products?"*
   - *"Compare Q4 2023 vs Q4 2024 revenue by channel"*
   - *"Which product category has the highest gross margin?"*
   - *"How many new customers were acquired each month this year?"*
   - *"Which stores are below 80% of their monthly target?"*

See `IMPROVEMENTS.md` for the full Genie demo script, trusted asset SQL, and system prompt instructions.

## Exploring Governance Features

### Tags
Open **Data Explorer** → your catalog → any table → **Tags** tab to see applied tags.

### Lineage
Click the **Lineage** tab on any mart view to see its upstream source tables visualised as a graph.

### Column Masks
Query `dim_customer.email` — if you are not a member of the `pii_readers` Unity Catalog group, you will see masked values (`a***@gmail.com`).

### Row Filters
`dim_customer` has a row filter — non-PII roles only see `is_active = TRUE` customers.

### ML Model
Navigate to **ML** → **Models** → select your catalog → find `churn_predictor`. The model is registered with a `champion` alias and includes feature importance and performance metrics logged via MLflow.

## Quick-Start SQL

```sql
-- Top 10 stores by net revenue in 2024
SELECT store_name, store_region, SUM(net_revenue) AS revenue
FROM sample_db.mart.v_sales_summary
WHERE year = 2024
GROUP BY store_name, store_region
ORDER BY revenue DESC LIMIT 10;

-- Monthly revenue with YoY comparison
SELECT year, month_name, month_num,
       SUM(net_revenue) AS revenue,
       LAG(SUM(net_revenue), 12) OVER (ORDER BY year, month_num) AS prev_year_revenue,
       ROUND((SUM(net_revenue) - LAG(SUM(net_revenue), 12) OVER (ORDER BY year, month_num))
             / LAG(SUM(net_revenue), 12) OVER (ORDER BY year, month_num) * 100, 1) AS yoy_growth_pct
FROM sample_db.mart.v_sales_summary
GROUP BY year, month_name, month_num ORDER BY year, month_num;

-- Customers at churn risk ranked by lifetime value
SELECT customer_id, full_name, loyalty_tier, lifetime_value,
       days_since_last_order, preferred_category, risk_tier
FROM sample_db.mart.v_customer_360 c
JOIN sample_db.ml.churn_predictions p USING (customer_id)
WHERE p.risk_tier IN ('High', 'Critical')
ORDER BY lifetime_value DESC LIMIT 25;

-- Product return rates by category (requires finance_role for margin)
SELECT category, sub_category,
       SUM(units_sold) AS units_sold,
       SUM(return_quantity) AS returns,
       ROUND(AVG(return_rate_pct), 2) AS avg_return_rate_pct,
       SUM(revenue) AS total_revenue
FROM sample_db.mart.v_product_performance
WHERE year = 2024
GROUP BY category, sub_category
ORDER BY avg_return_rate_pct DESC;

-- Inventory items below reorder point right now
SELECT p.product_name, p.category, s.store_name, s.region,
       i.quantity_available, i.reorder_point, i.quantity_on_order
FROM sample_db.facts.fact_inventory i
JOIN sample_db.dimensions.dim_product p ON i.product_key = p.product_key
JOIN sample_db.dimensions.dim_store   s ON i.store_key   = s.store_key
WHERE i.reorder_triggered = TRUE
  AND i.snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM sample_db.facts.fact_inventory)
ORDER BY i.quantity_available ASC;
```

## Teardown

To remove all created objects:

```sql
DROP CATALOG IF EXISTS sample_db CASCADE;
```

Or re-run `00_main` with `reset_catalog = true`.
