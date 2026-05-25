# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Fact Tables
# MAGIC
# MAGIC Creates and populates all **four fact tables** with synthetic transactional data
# MAGIC derived from the dimension tables created in notebook 02:
# MAGIC
# MAGIC | Table | Rows (approx) | Description |
# MAGIC |-------|--------------|-------------|
# MAGIC | `fact_orders` | 50,000 | Customer orders with totals and channel |
# MAGIC | `fact_order_items` | ~130,000 | Individual line items per order |
# MAGIC | `fact_inventory` | ~36,000 | Monthly inventory snapshots per store/product |
# MAGIC | `fact_returns` | ~2,500 | Return transactions linked to original orders |
# MAGIC
# MAGIC Data is generated with **realistic seasonality** (Q4 peak) and **referential integrity**
# MAGIC against all dimension tables.

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "sample_db", "Catalog Name")
dbutils.widgets.text("env",           "dev",        "Environment")
dbutils.widgets.text("reset_catalog", "false",      "Reset")
dbutils.widgets.text("num_customers", "2000",       "Num Customers")
dbutils.widgets.text("num_orders",    "50000",      "Num Orders")

CATALOG    = dbutils.widgets.get("catalog_name")
NUM_ORDERS = int(dbutils.widgets.get("num_orders"))
DIM        = f"{CATALOG}.dimensions"
FACT       = f"{CATALOG}.facts"

print(f"Target: {FACT}  |  Orders: {NUM_ORDERS:,}")

# COMMAND ----------

import random
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from pyspark.sql import functions as F

random.seed(42)
np.random.seed(42)

# COMMAND ----------

# MAGIC %md ### Load dimension keys

# COMMAND ----------

# Load dimension tables as pandas for fast sampling
customers_pdf  = spark.table(f"{DIM}.dim_customer").select("customer_key", "customer_id", "region").toPandas()
products_pdf   = spark.table(f"{DIM}.dim_product").select("product_key", "unit_price", "unit_cost", "category").toPandas()
stores_pdf     = spark.table(f"{DIM}.dim_store").select("store_key", "store_type", "region").toPandas()
employees_pdf  = spark.table(f"{DIM}.dim_employee").select("employee_key").toPandas()
promos_pdf     = spark.table(f"{DIM}.dim_promotion").select("promotion_key", "discount_rate", "applicable_category").toPandas()
date_keys_pdf  = (spark.table(f"{DIM}.dim_date")
                       .filter("full_date BETWEEN '2022-01-01' AND '2024-12-31'")
                       .select("date_key", "full_date", "month_num", "calendar_year", "is_weekend")
                       .toPandas())

# Add seasonality weights to date_keys (higher in Q4 and summer)
def month_weight(m):
    weights = {1: 6, 2: 5, 3: 7, 4: 8, 5: 8, 6: 9,
               7: 7, 8: 7, 9: 8, 10: 9, 11: 13, 12: 13}
    return weights.get(m, 7)

date_keys_pdf["weight"] = date_keys_pdf["month_num"].map(month_weight)
date_weights = date_keys_pdf["weight"].values.astype(float)
date_weights /= date_weights.sum()

customer_keys  = customers_pdf["customer_key"].values
store_keys     = stores_pdf["store_key"].values
employee_keys  = employees_pdf["employee_key"].values

# Online store (store_key=15) gets ~35% of orders
store_weights  = np.where(stores_pdf["store_type"] == "Online", 0.35 / len(stores_pdf[stores_pdf["store_type"]=="Online"]),
                          0.65 / len(stores_pdf[stores_pdf["store_type"]!="Online"]))
store_weights /= store_weights.sum()

product_keys   = products_pdf["product_key"].values
product_prices = products_pdf["unit_price"].values
product_costs  = products_pdf["unit_cost"].values

# Guard against null unit_cost — can occur if dim_product was read while a column mask was
# active on a previous run, or if the table was partially recreated.  Fall back to the same
# per-category cost ratios used in 02_dimension_tables so the values are realistic.
_null_cost_mask = pd.isna(product_costs)
if _null_cost_mask.any():
    _CATEGORY_COST_RATIOS = {
        "Electronics":        0.55,
        "Clothing & Apparel": 0.40,
        "Food & Beverage":    0.30,
        "Home & Living":      0.50,
        "Sports & Outdoors":  0.48,
    }
    _fallback_costs = (
        products_pdf["unit_price"]
        * products_pdf["category"].map(_CATEGORY_COST_RATIOS).fillna(0.50)
    ).values
    product_costs = np.where(_null_cost_mask, _fallback_costs, product_costs)
    print(f"  WARNING: filled {int(_null_cost_mask.sum())} null unit_cost values "
          f"using category cost ratios — re-check dim_product if this is unexpected")

promo_keys_arr = promos_pdf["promotion_key"].values

print(f"Loaded {len(customer_keys):,} customers, {len(product_keys):,} products, "
      f"{len(store_keys):,} stores, {len(date_keys_pdf):,} date keys")

# COMMAND ----------

# MAGIC %md ## fact_orders

# COMMAND ----------

N = NUM_ORDERS

# Sample with replacement, using seasonality weights for dates
sampled_date_idx  = np.random.choice(len(date_keys_pdf), size=N, p=date_weights)
sampled_dates     = date_keys_pdf.iloc[sampled_date_idx]

order_date_keys   = sampled_dates["date_key"].values
order_date_strs   = sampled_dates["full_date"].values
sampled_cust_idx  = np.random.choice(len(customer_keys), size=N)
sampled_store_idx = np.random.choice(len(store_keys), size=N, p=store_weights)
sampled_emp_idx   = np.random.choice(len(employee_keys), size=N)

# Generate order-level amounts — these are later reconciled with order items
# Use a log-normal distribution for realistic right-skewed order totals
subtotals         = np.round(np.random.lognormal(mean=4.8, sigma=0.8, size=N), 2)  # median ~$120
subtotals         = np.clip(subtotals, 5.0, 8000.0)
has_discount      = np.random.random(N) < 0.32
discount_pcts     = np.where(has_discount, np.random.uniform(0.05, 0.35, N), 0.0)
discount_amounts  = np.round(subtotals * discount_pcts, 2)
tax_rates         = np.random.choice([0.0625, 0.075, 0.08, 0.0875, 0.10], size=N)
tax_amounts       = np.round((subtotals - discount_amounts) * tax_rates, 2)
shipping_amounts  = np.where(subtotals > 75, 0.0, np.random.choice([4.99, 7.99, 12.99], size=N))
total_amounts     = np.round(subtotals - discount_amounts + tax_amounts + shipping_amounts, 2)

# Order status distribution
status_choices  = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
order_statuses  = np.random.choice(status_choices, size=N)

# Channel weighted by store type
channels        = np.where(store_keys[sampled_store_idx] == 15,
                           np.random.choice(["Online", "Mobile App"], size=N),
                           np.random.choice(["In-Store", "In-Store", "Phone"], size=N))

payment_methods = np.random.choice(
    ["Credit Card", "Debit Card", "PayPal", "Cash", "Gift Card", "Apple Pay", "Buy Now Pay Later"],
    size=N, p=[0.35, 0.25, 0.15, 0.10, 0.05, 0.07, 0.03]
)
shipping_methods = np.random.choice(["Standard", "Express", "Same-Day", "Click & Collect"], size=N,
                                    p=[0.55, 0.25, 0.10, 0.10])

# Ship date = order date + 0-7 days (no ship date for Processing/Cancelled)
ship_day_offsets = np.random.randint(1, 8, size=N)
ship_date_keys   = np.where(
    np.isin(order_statuses, ["Delivered", "Shipped"]),
    order_date_keys + ship_day_offsets,   # approximate — date key arithmetic
    -1
)

promo_assignment = np.where(np.random.random(N) < 0.28,
                            np.random.choice(promo_keys_arr, size=N),
                            -1)

orders_dict = {
    "order_key":       np.arange(1, N + 1),
    "order_id":        [f"ORD-{i:07d}" for i in range(1, N + 1)],
    "customer_key":    customer_keys[sampled_cust_idx],
    "store_key":       store_keys[sampled_store_idx],
    "employee_key":    employee_keys[sampled_emp_idx],
    "order_date_key":  order_date_keys,
    "ship_date_key":   ship_date_keys,
    "order_status":    order_statuses,
    "channel":         channels,
    "payment_method":  payment_methods,
    "shipping_method": shipping_methods,
    "promotion_key":   promo_assignment,
    "subtotal":        subtotals,
    "discount_amount": discount_amounts,
    "tax_amount":      tax_amounts,
    "shipping_amount": shipping_amounts,
    "total_amount":    total_amounts,
    "currency":        "USD",
    "is_returned":     order_statuses == "Returned",
    "return_date_key": np.where(order_statuses == "Returned", order_date_keys + np.random.randint(5, 45, size=N), -1),
    "_created_at":     [datetime.now().isoformat()] * N,
    "_updated_at":     [datetime.now().isoformat()] * N,
}

orders_pdf = pd.DataFrame(orders_dict)

(spark.createDataFrame(orders_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{FACT}.fact_orders"))

spark.sql(f"ALTER TABLE {FACT}.fact_orders CLUSTER BY (order_date_key, customer_key)")
spark.sql(f"""ALTER TABLE {FACT}.fact_orders SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)""")
print(f"fact_orders: {N:,} rows")

# COMMAND ----------

# MAGIC %md ## fact_order_items

# COMMAND ----------

# Generate 1-5 items per order (triangular distribution, mode=2)
items_per_order = np.random.triangular(1, 2, 6, size=N).astype(int)
items_per_order = np.clip(items_per_order, 1, 5)
total_items     = int(items_per_order.sum())

# Repeat order keys to create item rows
item_order_keys  = np.repeat(orders_dict["order_key"], items_per_order)
item_order_ids   = np.repeat(orders_dict["order_id"],  items_per_order)
item_store_keys  = np.repeat(orders_dict["store_key"], items_per_order)  # for category filtering

# Sample products with price-weighted probability (cheaper products sell more)
inv_prices       = 1.0 / product_prices
inv_prices      /= inv_prices.sum()
sampled_prod_idx = np.random.choice(len(product_keys), size=total_items, p=inv_prices)
item_product_keys= product_keys[sampled_prod_idx]
item_unit_prices = product_prices[sampled_prod_idx]
item_unit_costs  = product_costs[sampled_prod_idx]

item_quantities  = np.random.choice([1, 1, 1, 2, 2, 3, 4, 5], size=total_items)
has_item_disc    = np.random.random(total_items) < 0.20
item_disc_pcts   = np.where(has_item_disc, np.random.uniform(0.05, 0.30, total_items), 0.0)
item_disc_amts   = np.round(item_unit_prices * item_quantities * item_disc_pcts, 2)
item_line_totals = np.round(item_unit_prices * item_quantities - item_disc_amts, 2)

items_dict = {
    "order_item_key": np.arange(1, total_items + 1),
    "order_key":      item_order_keys,
    "order_id":       item_order_ids,
    "product_key":    item_product_keys,
    "quantity":       item_quantities,
    "unit_price":     np.round(item_unit_prices, 2),
    "unit_cost":      np.round(item_unit_costs, 2),
    "discount_pct":   np.round(item_disc_pcts * 100, 2),
    "discount_amount":item_disc_amts,
    "line_total":     item_line_totals,
    "_created_at":    [datetime.now().isoformat()] * total_items,
}

items_pdf = pd.DataFrame(items_dict)

(spark.createDataFrame(items_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{FACT}.fact_order_items"))

spark.sql(f"ALTER TABLE {FACT}.fact_order_items CLUSTER BY (order_key, product_key)")
spark.sql(f"""ALTER TABLE {FACT}.fact_order_items SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
)""")
print(f"fact_order_items: {total_items:,} rows")

# COMMAND ----------

# MAGIC %md ## fact_inventory

# COMMAND ----------

# Monthly inventory snapshots: all active products × all physical stores × 24 months
snapshot_months = pd.date_range("2023-01-01", "2024-12-31", freq="MS")

physical_stores = stores_pdf[stores_pdf["store_type"] != "Online"]["store_key"].values
n_products      = len(product_keys)
n_stores        = len(physical_stores)
n_months        = len(snapshot_months)
n_inv           = n_products * n_stores * n_months

print(f"Generating {n_products} products × {n_stores} stores × {n_months} months = {n_inv:,} inventory rows …")

# Build inventory snapshot using broadcasting
prod_idx_arr  = np.tile(np.arange(n_products), n_stores * n_months)
store_idx_arr = np.repeat(np.tile(np.arange(n_stores), n_products), n_months)
month_idx_arr = np.tile(np.arange(n_months), n_products * n_stores)

inv_date_keys = np.array([
    int(d.strftime("%Y%m%d")) for d in snapshot_months
])[month_idx_arr]

inv_prod_keys  = product_keys[prod_idx_arr]
inv_store_keys = physical_stores[store_idx_arr]
inv_unit_costs = product_costs[prod_idx_arr]
inv_unit_prices= product_prices[prod_idx_arr]

# Simulate stock levels: higher-priced items have lower quantities
base_qty       = np.round(500.0 / (inv_unit_prices + 1) * 20 + np.random.randint(5, 50, n_inv)).astype(int)
base_qty       = np.clip(base_qty, 0, 500)
reserved_qty   = np.clip((base_qty * np.random.uniform(0, 0.15, n_inv)).astype(int), 0, base_qty)
available_qty  = base_qty - reserved_qty
on_order_qty   = np.where(available_qty < 20, np.random.randint(50, 200, n_inv), 0)
reorder_pts    = np.clip((inv_unit_prices / 5).astype(int), 10, 100)
reorder_triggered = available_qty < reorder_pts
inv_value      = np.round(base_qty * inv_unit_costs, 2)

inv_dict = {
    "inventory_key":       np.arange(1, n_inv + 1),
    "snapshot_date_key":   inv_date_keys,
    "product_key":         inv_prod_keys,
    "store_key":           inv_store_keys,
    "quantity_on_hand":    base_qty,
    "quantity_reserved":   reserved_qty,
    "quantity_available":  available_qty,
    "quantity_on_order":   on_order_qty,
    "reorder_point":       reorder_pts,
    "reorder_triggered":   reorder_triggered,
    "unit_cost":           np.round(inv_unit_costs, 2),
    "total_cost_value":    inv_value,
    "_created_at":         [datetime.now().isoformat()] * n_inv,
}

inv_pdf = pd.DataFrame(inv_dict)

(spark.createDataFrame(inv_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{FACT}.fact_inventory"))

spark.sql(f"ALTER TABLE {FACT}.fact_inventory CLUSTER BY (snapshot_date_key, store_key)")
spark.sql(f"""ALTER TABLE {FACT}.fact_inventory SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
)""")
print(f"fact_inventory: {n_inv:,} rows")

# COMMAND ----------

# MAGIC %md ## fact_returns

# COMMAND ----------

# Returns: ~5% of Delivered orders become returns
delivered_orders  = orders_pdf[orders_pdf["order_status"] == "Delivered"]
return_mask       = np.random.random(len(delivered_orders)) < 0.05
return_orders     = delivered_orders[return_mask].copy()
n_returns         = len(return_orders)

RETURN_REASONS    = ["Changed Mind", "Defective Product", "Wrong Item Received",
                     "Item Damaged", "Better Price Found", "Ordered by Mistake", "Other"]
RETURN_REASON_W   = [0.25, 0.20, 0.18, 0.15, 0.10, 0.07, 0.05]
REFUND_METHODS    = ["Original Payment Method", "Store Credit", "Gift Card", "Bank Transfer"]

return_rows = []
for idx, (_, row) in enumerate(return_orders.iterrows(), 1):
    reason = random.choices(RETURN_REASONS, RETURN_REASON_W)[0]
    refund_pct = random.uniform(0.7, 1.0) if reason == "Partial Wear" else 1.0
    return_rows.append({
        "return_key":       idx,
        "return_id":        f"RET-{idx:06d}",
        "order_key":        int(row["order_key"]),
        "order_id":         row["order_id"],
        "customer_key":     int(row["customer_key"]),
        "store_key":        int(row["store_key"]),
        "return_date_key":  int(row["order_date_key"]) + random.randint(3, 45),
        "return_reason":    reason,
        "refund_amount":    round(float(row["total_amount"]) * refund_pct, 2),
        "refund_method":    random.choice(REFUND_METHODS),
        "is_restocked":     random.random() > 0.30,
        "_created_at":      datetime.now().isoformat(),
    })

returns_pdf = pd.DataFrame(return_rows)

(spark.createDataFrame(returns_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{FACT}.fact_returns"))

spark.sql(f"ALTER TABLE {FACT}.fact_returns CLUSTER BY (return_date_key, customer_key)")
print(f"fact_returns: {n_returns:,} rows")

# COMMAND ----------

# MAGIC %md ### Summary

# COMMAND ----------

for tbl in ["fact_orders", "fact_order_items", "fact_inventory", "fact_returns"]:
    cnt = spark.table(f"{FACT}.{tbl}").count()
    print(f"  {FACT}.{tbl}: {cnt:,} rows")
print("\nAll fact tables created successfully.")
