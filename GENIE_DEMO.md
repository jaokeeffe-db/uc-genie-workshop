
## 4. Genie Demo Guide

### 4.1 Prerequisites

Before the demo:
1. Deploy the catalog using `00_main.py` (one-time setup, 10–20 min).
2. Run `08_genie_metadata_augmentation.py` to add the current-period view and metric definitions.
3. Run `09_demo_data_refresh.py` to make `agg_daily_sales` and ML predictions feel current.
4. Create the Genie Space (see Section 4.2).

### 4.2 Genie Space Setup

1. Navigate to **AI/BI** → **Genie** → **New Space**.
2. Name: **Northwind Retail Analytics**.
3. Description: *"AI assistant for Northwind Analytics retail data. Ask questions about revenue, customers, products, store performance, and churn risk."*
4. **Add tables** from the `mart` schema:
   - `mv_sales` *(added by notebook 11)* — revenue, orders, discount rate metric view
   - `mv_customers` *(added by notebook 11)* — customer health, LTV, churn risk metric view
   - `mv_products` *(added by notebook 11)* — product revenue, margin, return rate metric view
   - `mv_stores` *(added by notebook 11)* — store performance vs target metric view
   - `v_sales_summary` — historical daily revenue
   - `v_sales_current` *(added by notebook 08)* — date-shifted for current-period queries
   - `v_customer_360` — customer profiles and churn risk
   - `v_product_performance` — product sales and margins
   - `v_store_performance` — store KPIs vs targets
   - `v_employee_sales` — sales associate rankings
   - `v_cohort_analysis` — customer retention
   - `agg_daily_sales` — fast aggregates for trend queries
   - `v_kpi_executive` *(added by notebook 08)* — single-row KPI summary
   - `metric_definitions` *(added by notebook 08)* — business metric glossary

5. **System instructions** (paste into the Genie Space instructions field):

```
You are an AI analyst for Northwind Analytics, a multi-channel retail company selling electronics, clothing, food, home goods, and sporting goods across 14 physical stores and an online channel.

Key business context:
- Fiscal year starts in April (Q1 = Apr–Jun, Q2 = Jul–Sep, Q3 = Oct–Dec, Q4 = Jan–Mar)
- "Net revenue" = revenue after discounts. "Gross revenue" = revenue before discounts.
- Churn risk customers are those with no orders in 60+ days AND fewer than 2 orders in the last 90 days.
- Store targets are annual figures stored in dim_store; v_store_performance prorates them monthly.
- The Online store (store_type = 'Online') receives approximately 35% of all orders across all channels.
- Loyalty tiers: Bronze < Silver < Gold < Platinum (Platinum is the highest tier).
- Customer segments: Consumer (individual shoppers), SMB (small businesses), Enterprise (large companies).
- The data covers orders from January 2022 onwards. Use v_sales_current for current-period analysis.

When asked about revenue trends, orders, or discount rates, prefer mv_sales (metric view).
When asked about customer counts, churn risk, or lifetime value, prefer mv_customers (metric view).
When asked about product performance, margins, or return rates, prefer mv_products (metric view).
When asked about store performance or target attainment, prefer mv_stores (metric view).
When asked about "this month", "this quarter", or "this year", use v_sales_current or agg_daily_sales for revenue queries.
When asked about individual customer profiles or detailed churn risk, use v_customer_360.
When asked about a specific metric definition, check metric_definitions first.
```

6. **Add the six Example SQL query snippets below:

```sql
-- Trusted Asset 1: Revenue by period
-- Intent: "revenue last quarter / this month / YTD"
SELECT
    year_quarter,
    SUM(net_revenue)          AS total_revenue,
    SUM(gross_revenue)        AS gross_revenue,
    SUM(total_discounts)      AS total_discounts,
    SUM(num_orders)           AS order_count
FROM mart.v_sales_summary
GROUP BY year_quarter
ORDER BY year_quarter DESC;

-- Trusted Asset 2: Store performance vs target
-- Intent: "which stores are underperforming / top stores"
SELECT
    store_name,
    region,
    SUM(actual_revenue)       AS ytd_revenue,
    MAX(annual_target)        AS annual_target,
    SUM(actual_revenue) / MAX(annual_target) * 100  AS pct_of_annual_target,
    SUM(revenue_vs_target)    AS total_variance
FROM mart.v_store_performance
WHERE year = (SELECT MAX(year) FROM mart.v_store_performance)
GROUP BY store_name, region
ORDER BY pct_of_annual_target ASC;

-- Trusted Asset 3: Churn risk customers
-- Intent: "customers at churn risk / who might churn"
SELECT
    customer_id,
    first_name || ' ' || last_name AS customer_name,
    loyalty_tier,
    customer_segment,
    lifetime_value,
    days_since_last_order,
    orders_last_90d,
    preferred_category
FROM mart.v_customer_360
WHERE is_churn_risk = TRUE
ORDER BY lifetime_value DESC;

-- Trusted Asset 4: Top products by revenue
-- Intent: "best selling products / top products this month"
SELECT
    product_name,
    category,
    sub_category,
    SUM(units_sold)           AS units_sold,
    SUM(revenue)              AS total_revenue,
    AVG(gross_margin_pct)     AS avg_margin_pct,
    SUM(return_count)         AS returns
FROM mart.v_product_performance
GROUP BY product_name, category, sub_category
ORDER BY total_revenue DESC
LIMIT 20;

-- Trusted Asset 5: Return rate by category
-- Intent: "return rates / which products get returned"
SELECT
    category,
    sub_category,
    SUM(units_sold)           AS units_sold,
    SUM(return_quantity)      AS returns,
    SUM(return_quantity) / NULLIF(SUM(units_sold), 0) * 100 AS return_rate_pct
FROM mart.v_product_performance
GROUP BY category, sub_category
ORDER BY return_rate_pct DESC;

-- Trusted Asset 6: Customer cohort retention
-- Intent: "cohort retention / how long do customers stay"
SELECT
    cohort_month,
    period_number,
    AVG(retention_rate_pct)   AS avg_retention_pct,
    SUM(cohort_revenue)       AS total_cohort_revenue
FROM mart.v_cohort_analysis
WHERE period_number <= 12
GROUP BY cohort_month, period_number
ORDER BY cohort_month, period_number;
```

### 4.3 Demo Script — Narrative Arc

The most effective demo tells a business story: *"Our CEO wants a quarterly business review. Let's use Genie to build it in real time."*

**Act 1 — Revenue Overview** (2 min)

> *"Let's start with the big picture. Genie, how did we do last quarter?"*

Questions to ask:
1. `"What was our total net revenue last quarter?"` → Genie returns a number with a chart.
2. `"How does that compare to the same quarter last year?"` → Genie adds a YoY comparison.
3. `"Break it down by region."` → Genie adds a regional bar chart.
4. `"Which store is performing best?"` → Uses `v_store_performance`.

**Point to highlight**: Genie understood "last quarter" relative to today, aggregated across channels, and connected data across three tables without any SQL.

---

**Act 2 — Customer Health** (2 min)

> *"Good revenue numbers, but let's look at customer retention — that's where we find early warning signs."*

5. `"How many customers are at risk of churning?"` → Uses `v_customer_360` with `is_churn_risk = TRUE`.
6. `"What's the total revenue at risk from churning customers?"` → Sum of `lifetime_value` for churn-risk customers.
7. `"Which loyalty tier has the highest churn risk?"` → Genie groups by `loyalty_tier`.
8. `"Show me the top 10 at-risk customers sorted by lifetime value."` → Uses trusted asset #3.

**Point to highlight**: Genie connected behavioral recency data with customer value to prioritise outreach — a multi-step analytical query expressed in plain English.

---

**Act 3 — Product Performance** (2 min)

> *"Let's dig into what's selling and what's coming back."*

9. `"What are our top 5 products by revenue?"` → Uses trusted asset #4.
10. `"Which product category has the highest return rate?"` → Uses trusted asset #5.
11. `"Show me Electronics products with a return rate above 10%."` → Filtered product view.
12. `"What's the gross margin on Sports & Outdoors vs Electronics?"` → Margin comparison.

**Point to highlight**: Questions cascade naturally, each building on the last — this is a conversation, not a series of isolated SQL queries.

---

**Act 4 — Governance Demo** (1 min)

> *"Now let me show you that Genie respects your data governance policies."*

13. `"Show me customer emails for the top 10 churning customers."` → Genie queries `v_customer_360.email`.

**What to show**:
- If your demo account is NOT in `pii_readers` group, emails appear as `j***@gmail.com`.
- Open the SQL Editor and run the same query directly — same masking applies.
- Switch to a user who IS in `pii_readers` — full emails appear.

**Point to highlight**: Column masks apply at the Unity Catalog layer, not the application layer. Genie, SQL Editor, and third-party BI tools all see the same masked values.

---

**Act 5 — ML Predictions** (1 min)

> *"Finally, let's connect this to our AI model."*

14. `"Which customers does our churn prediction model rate as Critical risk?"` → Joins `ml.churn_predictions`.
15. `"What's the average churn probability for Enterprise segment customers?"` → Group by segment.

> *"The churn_predictor model is registered in Unity Catalog with full lineage — you can see which training data it was built on, who deployed it, and when."*

**Point to highlight**: Navigate to **Catalog** → `sample_db` → `ml` → `churn_predictor` → **Lineage** tab. Show the model's feature inputs and the training run in MLflow.

---

**Act 6 — Cohort & Retention** (optional, 1 min)

16. `"What's the 3-month retention rate for customers acquired in 2023?"` → Uses `v_cohort_analysis`.
17. `"Which acquisition channel has the best 6-month retention?"` → Groups by `acquisition_channel`.

---

### 4.4 Governance Demo — Detailed Setup

To demonstrate column masks and row filters live:

1. **Create demo users**: Create two Databricks users — `analyst@demo.com` and `pii_analyst@demo.com`.
2. **Create groups in Unity Catalog**:
   ```sql
   -- Run in SQL Editor as account admin
   CREATE GROUP pii_readers;
   CREATE GROUP finance_role;
   CREATE GROUP hr_role;
   ```
3. **Add the PII user to the group**:
   ```sql
   ALTER GROUP pii_readers ADD USER `pii_analyst@demo.com`;
   ```
4. **Grant SELECT on the catalog**:
   ```sql
   GRANT USE CATALOG ON CATALOG sample_db TO `analyst@demo.com`;
   GRANT USE SCHEMA ON SCHEMA sample_db.mart TO `analyst@demo.com`;
   GRANT SELECT ON TABLE sample_db.mart.v_customer_360 TO `analyst@demo.com`;
   -- repeat for pii_analyst@demo.com
   GRANT USE CATALOG ON CATALOG sample_db TO `pii_analyst@demo.com`;
   GRANT USE SCHEMA ON SCHEMA sample_db.mart TO `pii_analyst@demo.com`;
   GRANT SELECT ON TABLE sample_db.mart.v_customer_360 TO `pii_analyst@demo.com`;
   ```
5. Open two browser windows — log in as each user and run:
   ```sql
   SELECT customer_id, email FROM sample_db.dimensions.dim_customer LIMIT 10;
   ```
   The `analyst` sees `j***@gmail.com`; the `pii_analyst` sees `jane.doe@gmail.com`.

---

### 4.5 Genie Question Bank

Copy these questions into the Genie Space's **Example Questions** section so they appear as suggestions:

**Revenue**
- "What was total net revenue in 2024?"
- "Show monthly revenue for the last 12 months with a trend line."
- "Which quarter had the highest gross revenue?"
- "What percentage of orders used a promotion discount?"
- "Compare Q4 2023 vs Q4 2024 revenue."

**Customers**
- "How many customers are at churn risk right now?"
- "Who are our top 10 customers by lifetime value?"
- "What's the average order value for Enterprise vs Consumer customers?"
- "Which acquisition channel brings in the most valuable customers?"
- "What is the retention rate for customers who joined in January 2023?"

**Products & Inventory**
- "Which 5 products have the highest return rate?"
- "What is the gross margin for each category?"
- "Show me slow-moving products with low stock levels."
- "Which brand generates the most revenue?"

**Stores & Operations**
- "Which stores are below 80% of their monthly target?"
- "What is revenue per square foot for flagship stores?"
- "Show me store performance ranked by revenue vs target."
- "Which region grew the most year-over-year?"

**ML & Predictions**
- "How many customers are classified as Critical churn risk?"
- "What is the average churn probability for Platinum tier customers?"
- "Show me high-value customers predicted to churn."

---

### 4.6 Common Genie Failure Modes and Fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| Genie returns empty results for "this month" | All fact data ends 2024-12-31 | Run `08_genie_metadata_augmentation.py`, use `v_sales_current` in the space |
| Genie uses the wrong table for revenue | Multiple views have `net_revenue` | Add Genie system instructions specifying which table to prefer for which question type |
| Genie can't find churn risk | `is_churn_risk` not in `agg_daily_sales` | Point Genie at `v_customer_360` for customer questions |
| Column mask demo not working | Groups `pii_readers` / `finance_role` don't exist | Create them manually with `CREATE GROUP` in SQL Editor |
| ML predictions show old `scored_at` timestamp | `churn_predictions` table created at deploy time | Run `09_demo_data_refresh.py` to update timestamps |
| Genie invents metric definitions | No `metric_definitions` table | Run `08_genie_metadata_augmentation.py` to add the glossary table |
| Metric view creation fails | Cluster runtime too old | Metric views require DBR 15.4 LTS+ or a Serverless SQL warehouse |
| Genie gives inconsistent KPI answers | Metric views not in Genie Space | Add `mv_sales`, `mv_customers`, `mv_products`, `mv_stores` to the space |

---
