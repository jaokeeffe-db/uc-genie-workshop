# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "scikit-learn",
#   "mlflow",
# ]
# ///
# MAGIC %pip install scikit-learn mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — ML Feature Engineering & Model Registration
# MAGIC
# MAGIC Creates **ML feature tables** and registers a **churn prediction model** in the Unity Catalog Model Registry.
# MAGIC
# MAGIC | Object | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `ml.customer_features` | Feature Table | Pre-computed customer features for churn prediction |
# MAGIC | `ml.product_features` | Feature Table | Pre-computed product demand features |
# MAGIC | `ml.churn_predictor` | Registered Model | GBM churn prediction model (MLflow + UC) |
# MAGIC | `ml.churn_predictions` | Delta Table | Latest churn probability scores per customer |
# MAGIC | `ml.sales_forecast` | Delta Table | 30-day sales forecast by store and category |
# MAGIC
# MAGIC The churn model is registered with a **`champion`** alias pointing to the production version.

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "sample_db", "Catalog Name")
dbutils.widgets.text("env",           "dev",        "Environment")
dbutils.widgets.text("reset_catalog", "false",      "Reset")
dbutils.widgets.text("num_customers", "2000",       "Num Customers")
dbutils.widgets.text("num_orders",    "50000",      "Num Orders")

CATALOG = dbutils.widgets.get("catalog_name")
DIM     = f"{CATALOG}.dimensions"
FACT    = f"{CATALOG}.facts"
ML      = f"{CATALOG}.ml"

print(f"Target: {ML}")

# COMMAND ----------

# MAGIC %md ## customer_features

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {ML}.customer_features
CLUSTER BY (snapshot_date, customer_id)
COMMENT 'Customer feature table for churn prediction and lifetime value modelling.
Each row is a customer-snapshot combination. Snapshot date is the first day of each month.
Features cover the 90-day window ending on the snapshot date.
Primary join key: customer_id. Time key: snapshot_date.
Used by the churn_predictor model registered at {ML}.churn_predictor.'
AS
WITH ref_date AS (
    SELECT MAX(d.full_date) AS ref_dt
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key
),
base AS (
    SELECT
        c.customer_id,
        c.customer_key,
        date_trunc('month', (SELECT ref_dt FROM ref_date))           AS snapshot_date,
        c.loyalty_tier,
        c.segment,
        c.acquisition_channel,
        DATEDIFF((SELECT ref_dt FROM ref_date), c.registration_date) AS days_since_registration,
        COUNT(DISTINCT o.order_key)                                  AS total_orders_all_time,
        SUM(o.total_amount)                                          AS total_spend_all_time,
        COUNT(CASE WHEN d.full_date >= date_sub((SELECT ref_dt FROM ref_date), 90)
                   THEN o.order_key END)                             AS orders_last_90d,
        SUM(CASE WHEN d.full_date >= date_sub((SELECT ref_dt FROM ref_date), 90)
                 THEN o.total_amount ELSE 0 END)                     AS spend_last_90d,
        COUNT(CASE WHEN d.full_date >= date_sub((SELECT ref_dt FROM ref_date), 30)
                   THEN o.order_key END)                             AS orders_last_30d,
        AVG(o.total_amount)                                          AS avg_order_value,
        DATEDIFF((SELECT ref_dt FROM ref_date), MAX(d.full_date))    AS days_since_last_order,
        COUNT(DISTINCT o.store_key)                                  AS num_distinct_stores,
        SUM(CASE WHEN o.is_returned = TRUE THEN 1 ELSE 0 END) * 1.0
          / NULLIF(COUNT(DISTINCT o.order_key), 0)                   AS return_rate
    FROM {DIM}.dim_customer      c
    LEFT JOIN {FACT}.fact_orders  o  ON c.customer_key = o.customer_key
                                     AND o.order_status NOT IN ('Cancelled', 'Processing')
    LEFT JOIN {DIM}.dim_date      d  ON o.order_date_key = d.date_key
    GROUP BY c.customer_id, c.customer_key, c.loyalty_tier, c.segment,
             c.acquisition_channel, c.registration_date
),
preferred_category AS (
    SELECT
        c.customer_id,
        first_value(p.category) OVER (
            PARTITION BY c.customer_id
            ORDER BY COUNT(*) OVER (PARTITION BY c.customer_id, p.category) DESC
        )                                                            AS preferred_category
    FROM {FACT}.fact_orders      o
    JOIN {DIM}.dim_customer      c  ON o.customer_key  = c.customer_key
    JOIN {FACT}.fact_order_items i  ON o.order_key     = i.order_key
    JOIN {DIM}.dim_product       p  ON i.product_key   = p.product_key
    GROUP BY c.customer_id, p.category
)
SELECT
    b.customer_id,
    b.snapshot_date,
    b.loyalty_tier,
    b.segment,
    b.acquisition_channel,
    b.days_since_registration,
    COALESCE(b.total_orders_all_time, 0)                             AS total_orders_all_time,
    COALESCE(b.total_spend_all_time, 0.0)                            AS total_spend_all_time,
    COALESCE(b.orders_last_90d, 0)                                   AS orders_last_90d,
    COALESCE(b.spend_last_90d, 0.0)                                  AS spend_last_90d,
    COALESCE(b.orders_last_30d, 0)                                   AS orders_last_30d,
    COALESCE(b.avg_order_value, 0.0)                                 AS avg_order_value,
    COALESCE(b.days_since_last_order, 9999)                          AS days_since_last_order,
    COALESCE(b.num_distinct_stores, 0)                               AS num_distinct_stores,
    COALESCE(b.return_rate, 0.0)                                     AS return_rate,
    pc.preferred_category,
    -- Churn label: no orders in 60 days AND fewer than 2 orders in last 90 days
    (COALESCE(b.days_since_last_order, 9999) > 60
     AND COALESCE(b.orders_last_90d, 0) < 2)                        AS is_churn_label
FROM base b
LEFT JOIN preferred_category pc ON b.customer_id = pc.customer_id
""")

# CTAS does not preserve NOT NULL — set it explicitly before adding the PK constraint
spark.sql(f"ALTER TABLE {ML}.customer_features ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {ML}.customer_features ADD CONSTRAINT customer_features_pk PRIMARY KEY (customer_id)")

count = spark.table(f"{ML}.customer_features").count()
print(f"customer_features: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## product_features

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {ML}.product_features
CLUSTER BY (snapshot_date, product_id)
COMMENT 'Product feature table for demand forecasting and inventory optimisation models.
Each row is a product-snapshot combination with aggregated 30-day metrics.
Primary join key: product_id. Time key: snapshot_date.'
AS
WITH ref_date AS (
    SELECT MAX(d.full_date) AS ref_dt
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key
),
sales_30d AS (
    SELECT
        p.product_id,
        p.product_key,
        p.category,
        p.sub_category,
        p.brand,
        p.unit_price,
        COUNT(DISTINCT o.order_key)                                  AS orders_last_30d,
        SUM(i.quantity)                                              AS units_sold_last_30d,
        SUM(i.line_total)                                            AS revenue_last_30d,
        AVG(i.unit_price)                                            AS avg_selling_price_30d,
        SUM(i.discount_amount) / NULLIF(SUM(i.line_total + i.discount_amount), 0) AS avg_discount_rate_30d,
        COUNT(DISTINCT o.store_key)                                  AS num_stores_selling
    FROM {DIM}.dim_product        p
    LEFT JOIN {FACT}.fact_order_items i ON p.product_key = i.product_key
    LEFT JOIN {FACT}.fact_orders      o ON i.order_key   = o.order_key
    LEFT JOIN {DIM}.dim_date          d ON o.order_date_key = d.date_key
    WHERE d.full_date >= date_sub((SELECT ref_dt FROM ref_date), 30)
       OR d.full_date IS NULL
    GROUP BY p.product_id, p.product_key, p.category, p.sub_category, p.brand, p.unit_price
),
returns_30d AS (
    SELECT
        i.product_key,
        COUNT(DISTINCT r.return_key)                                 AS returns_last_30d,
        SUM(r.refund_amount)                                         AS refund_amount_last_30d
    FROM {FACT}.fact_returns       r
    JOIN {FACT}.fact_order_items   i ON r.order_key   = i.order_key
    JOIN {DIM}.dim_date            d ON r.return_date_key = d.date_key
    WHERE d.full_date >= date_sub((SELECT ref_dt FROM ref_date), 30)
    GROUP BY i.product_key
),
inventory_latest AS (
    SELECT
        product_key,
        AVG(quantity_available)                                      AS avg_stock_available,
        SUM(CAST(reorder_triggered AS INT)) * 1.0 / COUNT(*)        AS stores_below_reorder_pct
    FROM {FACT}.fact_inventory
    WHERE snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM {FACT}.fact_inventory)
    GROUP BY product_key
)
SELECT
    s.product_id,
    s.product_key,
    s.category,
    s.sub_category,
    s.brand,
    s.unit_price,
    date_trunc('month', (SELECT ref_dt FROM ref_date))               AS snapshot_date,
    COALESCE(s.orders_last_30d, 0)                                   AS orders_last_30d,
    COALESCE(s.units_sold_last_30d, 0)                               AS units_sold_last_30d,
    COALESCE(s.revenue_last_30d, 0.0)                                AS revenue_last_30d,
    COALESCE(s.avg_selling_price_30d, s.unit_price)                  AS avg_selling_price_30d,
    COALESCE(s.avg_discount_rate_30d, 0.0)                           AS avg_discount_rate_30d,
    COALESCE(s.num_stores_selling, 0)                                AS num_stores_selling,
    COALESCE(r.returns_last_30d, 0)                                  AS returns_last_30d,
    COALESCE(r.refund_amount_last_30d, 0.0)                          AS refund_amount_last_30d,
    COALESCE(r.returns_last_30d, 0) * 1.0 / NULLIF(s.units_sold_last_30d, 0) AS return_rate_30d,
    COALESCE(il.avg_stock_available, 0.0)                            AS avg_stock_available,
    COALESCE(il.stores_below_reorder_pct, 0.0)                       AS stores_below_reorder_pct,
    s.units_sold_last_30d >= 50                                      AS is_high_velocity,
    s.units_sold_last_30d = 0                                        AS is_slow_moving
FROM sales_30d     s
LEFT JOIN returns_30d     r  ON s.product_key = r.product_key
LEFT JOIN inventory_latest il ON s.product_key = il.product_key
""")

# CTAS does not preserve NOT NULL — set it explicitly before adding the PK constraint
spark.sql(f"ALTER TABLE {ML}.product_features ALTER COLUMN product_id SET NOT NULL")
spark.sql(f"ALTER TABLE {ML}.product_features ADD CONSTRAINT product_features_pk PRIMARY KEY (product_id)")

count = spark.table(f"{ML}.product_features").count()
print(f"product_features: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## Train and register churn prediction model

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score, recall_score
from sklearn.preprocessing import LabelEncoder
from mlflow.models.signature import infer_signature

# Configure MLflow to use Unity Catalog as the model registry
mlflow.set_registry_uri("databricks-uc")

# Use the current user's home directory for the experiment
current_user = spark.sql("SELECT current_user()").collect()[0][0]
experiment_path = f"/Users/{current_user}/northwind_churn_predictor"
mlflow.set_experiment(experiment_path)

print(f"MLflow experiment: {experiment_path}")
print(f"Model registry URI: databricks-uc")

# COMMAND ----------

# Prepare training data from the feature table
features_pdf = spark.table(f"{ML}.customer_features").toPandas()

# Encode categorical columns
categorical_cols = ["loyalty_tier", "segment", "acquisition_channel", "preferred_category"]
le_dict = {}
for col in categorical_cols:
    le = LabelEncoder()
    features_pdf[col + "_enc"] = le.fit_transform(features_pdf[col].fillna("Unknown"))
    le_dict[col] = le

feature_cols = [
    "days_since_registration", "total_orders_all_time", "total_spend_all_time",
    "orders_last_90d", "spend_last_90d", "orders_last_30d", "avg_order_value",
    "days_since_last_order", "num_distinct_stores", "return_rate",
    "loyalty_tier_enc", "segment_enc", "acquisition_channel_enc", "preferred_category_enc",
]

X = features_pdf[feature_cols].fillna(0)
y = features_pdf["is_churn_label"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42, stratify=y)
print(f"Training set: {len(X_train):,} | Test set: {len(X_test):,} | Churn rate: {y.mean():.1%}")

# COMMAND ----------

# Train the model and log to MLflow
model_name = f"{CATALOG}.ml.churn_predictor"

with mlflow.start_run(run_name="churn_gbm_v1") as run:
    # Hyperparameters
    params = {
        "n_estimators":   150,
        "max_depth":       4,
        "learning_rate":   0.08,
        "subsample":       1.0,
        "min_samples_leaf": 20,
        "random_state":    42,
    }
    mlflow.log_params(params)
    mlflow.set_tags({
        "model_type":    "churn_prediction",
        "framework":     "scikit-learn",
        "algorithm":     "GradientBoostingClassifier",
        "feature_table": f"{ML}.customer_features",
        "target":        "is_churn_label",
        "team":          "data_science",
    })

    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # Evaluate
    y_pred      = model.predict(X_test)
    y_prob      = model.predict_proba(X_test)[:, 1]
    accuracy    = accuracy_score(y_test, y_pred)
    roc_auc     = roc_auc_score(y_test, y_prob)
    precision   = precision_score(y_test, y_pred, zero_division=0)
    recall      = recall_score(y_test, y_pred, zero_division=0)

    mlflow.log_metrics({
        "accuracy":  round(accuracy, 4),
        "roc_auc":   round(roc_auc, 4),
        "precision": round(precision, 4),
        "recall":    round(recall, 4),
    })

    print(f"Model metrics — Accuracy: {accuracy:.3f} | ROC-AUC: {roc_auc:.3f} | "
          f"Precision: {precision:.3f} | Recall: {recall:.3f}")

    # Log feature importance
    fi_pdf = pd.DataFrame({
        "feature":   feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    mlflow.log_table(fi_pdf, "feature_importance.json")

    # Register model to Unity Catalog
    signature = infer_signature(X_train, model.predict_proba(X_train)[:, 1])
    model_info = mlflow.sklearn.log_model(
        sk_model           = model,
        artifact_path      = "churn_model",
        registered_model_name = model_name,
        signature          = signature,
        input_example      = X_train.head(5).astype(float),
    )

    run_id = run.info.run_id
    print(f"Model registered: {model_name}  (run_id: {run_id})")

# COMMAND ----------

# Alias the latest version as 'champion'
from mlflow.tracking import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")

# Get the latest version
versions = client.search_model_versions(f"name='{model_name}'")
latest_version = max(versions, key=lambda v: int(v.version)).version

client.set_registered_model_alias(
    name    = model_name,
    alias   = "champion",
    version = latest_version,
)

client.update_registered_model(
    name        = model_name,
    description = (
        "Gradient Boosting Machine for 30-day customer churn prediction. "
        "Trained on customer_features table. "
        "champion alias = current production model. "
        f"ROC-AUC: {roc_auc:.3f}. "
        "Input: 14 behavioural and demographic features. "
        "Output: probability of churn in next 30 days (0.0–1.0)."
    ),
)

client.update_model_version(
    name        = model_name,
    version     = latest_version,
    description = (
        f"v{latest_version}: Initial GBM baseline. "
        f"ROC-AUC={roc_auc:.3f}, Accuracy={accuracy:.3f}. "
        "Deployed to champion alias."
    ),
)

print(f"Model '{model_name}' version {latest_version} aliased as 'champion'")

# COMMAND ----------

# MAGIC %md ## churn_predictions (scored output table)

# COMMAND ----------

# Score all customers and write predictions table
loaded_model = mlflow.sklearn.load_model(f"models:/{model_name}@champion")

features_pdf["churn_probability"]  = loaded_model.predict_proba(X.values)[:, 1]
features_pdf["churn_prediction"]   = (features_pdf["churn_probability"] >= 0.5).astype(bool)
features_pdf["risk_tier"]          = pd.cut(
    features_pdf["churn_probability"],
    bins   = [0, 0.30, 0.50, 0.70, 1.01],
    labels = ["Low", "Medium", "High", "Critical"],
)
features_pdf["scored_at"]          = pd.Timestamp.now()
features_pdf["model_version"]      = latest_version
features_pdf["model_name"]         = model_name

predictions_cols = [
    "customer_id", "snapshot_date", "churn_probability", "churn_prediction",
    "risk_tier", "days_since_last_order", "orders_last_90d", "spend_last_90d",
    "loyalty_tier", "segment", "scored_at", "model_version", "model_name",
]

predictions_pdf = features_pdf[predictions_cols].copy()
predictions_pdf["churn_probability"] = predictions_pdf["churn_probability"].round(4)

(spark.createDataFrame(predictions_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{ML}.churn_predictions"))

spark.sql(f"ALTER TABLE {ML}.churn_predictions CLUSTER BY (snapshot_date, risk_tier)")
print(f"churn_predictions: {len(predictions_pdf):,} rows written")

# COMMAND ----------

# MAGIC %md ## sales_forecast (simple moving-average baseline)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {ML}.sales_forecast
CLUSTER BY (forecast_date, store_name)
COMMENT 'Sales forecast table using a 28-day moving average baseline model.
Each row is a store × category × forecast_date combination.
forecast_horizon_days = number of days ahead the forecast is for (1-30).
This is a statistical baseline model; a full ML forecast model would replace these predictions.
Used to demonstrate Unity Catalog model output tables and Genie forecast questions.'
AS
WITH ref_date AS (
    SELECT MAX(d.full_date) AS ref_dt
    FROM {FACT}.fact_orders o
    JOIN {DIM}.dim_date d ON o.order_date_key = d.date_key
),
daily_actuals AS (
    SELECT
        d.full_date,
        s.store_name,
        s.region,
        p.category,
        SUM(i.line_total)    AS daily_revenue,
        SUM(i.quantity)      AS daily_units
    FROM {FACT}.fact_order_items  i
    JOIN {FACT}.fact_orders       o ON i.order_key      = o.order_key
    JOIN {DIM}.dim_date           d ON o.order_date_key = d.date_key
    JOIN {DIM}.dim_store          s ON o.store_key      = s.store_key
    JOIN {DIM}.dim_product        p ON i.product_key    = p.product_key
    WHERE d.full_date >= date_sub((SELECT ref_dt FROM ref_date), 60)
    GROUP BY d.full_date, s.store_name, s.region, p.category
),
moving_avg AS (
    SELECT
        store_name,
        region,
        category,
        AVG(daily_revenue)   AS avg_daily_revenue,
        AVG(daily_units)     AS avg_daily_units,
        STDDEV(daily_revenue) AS stddev_revenue
    FROM daily_actuals
    GROUP BY store_name, region, category
)
SELECT
    store_name,
    region,
    category,
    date_add((SELECT ref_dt FROM ref_date), h.horizon)  AS forecast_date,
    h.horizon                                        AS forecast_horizon_days,
    ROUND(avg_daily_revenue * (1 + 0.03 * h.horizon / 30.0), 2) AS forecast_revenue,
    ROUND(avg_daily_units * (1 + 0.02 * h.horizon / 30.0), 2)   AS forecast_units,
    ROUND(avg_daily_revenue * 0.10, 2)              AS forecast_lower_bound,
    ROUND(avg_daily_revenue * 1.20, 2)              AS forecast_upper_bound,
    'moving_average_28d'                             AS model_type,
    current_date()                                   AS generated_date
FROM moving_avg
CROSS JOIN (SELECT explode(sequence(1, 30)) AS horizon) h
""")

count = spark.table(f"{ML}.sales_forecast").count()
print(f"sales_forecast: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

for tbl in ["customer_features", "product_features", "churn_predictions", "sales_forecast"]:
    cnt = spark.table(f"{ML}.{tbl}").count()
    print(f"  {ML}.{tbl}: {cnt:,} rows")

print(f"\nRegistered model: {model_name} (alias: champion)")
print("\nAll ML objects created successfully.")
