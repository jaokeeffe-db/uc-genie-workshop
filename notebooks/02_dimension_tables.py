# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Dimension Tables
# MAGIC
# MAGIC Creates and populates all **six dimension tables** with realistic synthetic data:
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `dim_date` | 2,922 | Calendar dates 2020-01-01 → 2027-12-31 |
# MAGIC | `dim_customer` | configurable (default 2,000) | Customer master with demographics and loyalty |
# MAGIC | `dim_product` | 200 | Product catalogue across 5 categories |
# MAGIC | `dim_store` | 15 | Retail store locations across regions |
# MAGIC | `dim_employee` | 50 | Store staff and managers |
# MAGIC | `dim_promotion` | 30 | Promotional campaigns and discounts |

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "sample_db", "Catalog Name")
dbutils.widgets.text("env",           "dev",        "Environment")
dbutils.widgets.text("reset_catalog", "false",      "Reset")
dbutils.widgets.text("num_customers", "2000",       "Num Customers")
dbutils.widgets.text("num_orders",    "50000",      "Num Orders")

CATALOG       = dbutils.widgets.get("catalog_name")
NUM_CUSTOMERS = int(dbutils.widgets.get("num_customers"))
DIM           = f"{CATALOG}.dimensions"

print(f"Target: {DIM}  |  Customers: {NUM_CUSTOMERS}")

# COMMAND ----------

import random
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta

random.seed(42)
np.random.seed(42)

# COMMAND ----------

# MAGIC %md ## dim_date

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM}.dim_date
COMMENT 'Date dimension spanning 2020-01-01 to 2027-12-31. Used as a foreign key target for all date columns in fact tables. Fiscal year begins in April.'
AS
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT)                           AS date_key,
    d                                                                   AS full_date,
    CAST(dayofweek(d) AS TINYINT)                                      AS day_of_week,
    date_format(d, 'EEEE')                                             AS day_name,
    CAST(dayofmonth(d) AS TINYINT)                                     AS day_of_month,
    CAST(dayofyear(d) AS SMALLINT)                                     AS day_of_year,
    CAST(weekofyear(d) AS TINYINT)                                     AS week_of_year,
    CAST(month(d) AS TINYINT)                                          AS month_num,
    date_format(d, 'MMMM')                                             AS month_name,
    date_format(d, 'MMM')                                              AS month_short,
    CAST(quarter(d) AS TINYINT)                                        AS calendar_quarter,
    CAST(year(d) AS SMALLINT)                                          AS calendar_year,
    CONCAT(year(d), '-Q', quarter(d))                                  AS year_quarter,
    CONCAT(year(d), '-', date_format(d, 'MM'))                         AS year_month,
    dayofweek(d) IN (1, 7)                                             AS is_weekend,
    CASE WHEN month(d) IN (11, 12) AND dayofmonth(d) IN (24, 25, 26, 31)
         THEN TRUE
         WHEN month(d) = 1 AND dayofmonth(d) = 1 THEN TRUE
         ELSE FALSE END                                                 AS is_holiday,
    NOT (dayofweek(d) IN (1, 7) OR
         (month(d) IN (11, 12) AND dayofmonth(d) IN (24, 25, 26, 31)) OR
         (month(d) = 1 AND dayofmonth(d) = 1))                        AS is_business_day,
    CAST(CASE WHEN month(d) >= 4 THEN quarter(d) - 1
              ELSE quarter(d) + 3 END AS TINYINT)                      AS fiscal_quarter,
    CAST(CASE WHEN month(d) >= 4 THEN year(d)
              ELSE year(d) - 1 END AS SMALLINT)                        AS fiscal_year,
    CONCAT('FY', CASE WHEN month(d) >= 4 THEN year(d) ELSE year(d) - 1 END,
           '-Q', CASE WHEN month(d) >= 4 THEN quarter(d) - 1 ELSE quarter(d) + 3 END)
                                                                        AS fiscal_year_quarter
FROM (
    SELECT explode(sequence(date'2020-01-01', date'2027-12-31', interval 1 day)) AS d
)
""")

count = spark.table(f"{DIM}.dim_date").count()
print(f"dim_date: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## dim_customer

# COMMAND ----------

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "James",
    "Isabella", "Oliver", "Mia", "Benjamin", "Charlotte", "Elijah", "Amelia",
    "Lucas", "Harper", "Mason", "Evelyn", "Logan", "Abigail", "Ethan", "Emily",
    "Alexander", "Elizabeth", "Henry", "Sofia", "Sebastian", "Ella", "Jack",
    "Scarlett", "Owen", "Grace", "Aiden", "Chloe", "Michael", "Victoria", "Daniel",
    "Riley", "Matthew", "Aria", "David", "Penelope", "Nathan", "Luna", "Joseph",
    "Layla", "Carter", "Nora", "Jayden", "Zoe", "Dylan", "Hannah", "Wyatt",
    "Lillian", "Gabriel", "Addison", "Julian", "Aubrey", "Levi", "Ellie", "Isaac",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Turner", "Phillips", "Evans", "Collins", "Edwards", "Stewart",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com", "proton.me"]

US_LOCATIONS = [
    ("New York", "NY", "United States", "Northeast", "10001"),
    ("Los Angeles", "CA", "United States", "West", "90001"),
    ("Chicago", "IL", "United States", "Midwest", "60601"),
    ("Houston", "TX", "United States", "South", "77001"),
    ("Phoenix", "AZ", "United States", "West", "85001"),
    ("Philadelphia", "PA", "United States", "Northeast", "19101"),
    ("San Antonio", "TX", "United States", "South", "78201"),
    ("San Diego", "CA", "United States", "West", "92101"),
    ("Dallas", "TX", "United States", "South", "75201"),
    ("San Jose", "CA", "United States", "West", "95101"),
    ("Austin", "TX", "United States", "South", "73301"),
    ("Seattle", "WA", "United States", "West", "98101"),
    ("Denver", "CO", "United States", "West", "80201"),
    ("Nashville", "TN", "United States", "South", "37201"),
    ("Charlotte", "NC", "United States", "South", "28201"),
    ("San Francisco", "CA", "United States", "West", "94102"),
    ("Indianapolis", "IN", "United States", "Midwest", "46201"),
    ("Columbus", "OH", "United States", "Midwest", "43085"),
    ("Portland", "OR", "United States", "West", "97201"),
    ("Las Vegas", "NV", "United States", "West", "89101"),
]

INTL_LOCATIONS = [
    ("London", "England", "United Kingdom", "International", "EC1A"),
    ("Manchester", "England", "United Kingdom", "International", "M1 1"),
    ("Birmingham", "England", "United Kingdom", "International", "B1 1"),
    ("Toronto", "Ontario", "Canada", "International", "M5H"),
    ("Vancouver", "British Columbia", "Canada", "International", "V6B"),
    ("Sydney", "NSW", "Australia", "International", "2000"),
    ("Melbourne", "VIC", "Australia", "International", "3000"),
    ("Dublin", "Leinster", "Ireland", "International", "D01"),
]

ALL_LOCATIONS = US_LOCATIONS * 6 + INTL_LOCATIONS  # ~75% US, 25% international

SEGMENTS          = ["Consumer", "Small Business", "Enterprise"]
SEGMENT_WEIGHTS   = [0.70, 0.20, 0.10]
LOYALTY_TIERS     = ["Bronze", "Silver", "Gold", "Platinum"]
LOYALTY_WEIGHTS   = [0.50, 0.30, 0.15, 0.05]
INCOME_BANDS      = ["<$30k", "$30k-$60k", "$60k-$100k", "$100k-$150k", ">$150k"]
INCOME_WEIGHTS    = [0.15, 0.30, 0.30, 0.15, 0.10]
ACQ_CHANNELS      = ["Online", "In-Store", "Referral", "Email Campaign", "Social Media", "Paid Search"]
ACQ_WEIGHTS       = [0.35, 0.25, 0.15, 0.10, 0.10, 0.05]
PAYMENT_METHODS   = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Gift Card"]

def rand_date(start_year=2018, end_year=2024):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_dob():
    return date(random.randint(1960, 2002), random.randint(1, 12), random.randint(1, 28))

rows = []
for i in range(1, NUM_CUSTOMERS + 1):
    fn        = random.choice(FIRST_NAMES)
    ln        = random.choice(LAST_NAMES)
    gender    = random.choice(["M", "M", "F", "F", "NB", "U"])
    email_sfx = random.choice(EMAIL_DOMAINS)
    email     = f"{fn.lower()}.{ln.lower()}{random.randint(1,999)}@{email_sfx}"
    loc       = random.choice(ALL_LOCATIONS)
    reg_date  = rand_date(2018, 2023)
    dob       = rand_dob()
    tier      = random.choices(LOYALTY_TIERS, LOYALTY_WEIGHTS)[0]
    pts_map   = {"Bronze": (0, 999), "Silver": (1000, 4999), "Gold": (5000, 19999), "Platinum": (20000, 99999)}
    pts_range = pts_map[tier]
    rows.append({
        "customer_key":         i,
        "customer_id":          f"CUST-{i:06d}",
        "first_name":           fn,
        "last_name":            ln,
        "full_name":            f"{fn} {ln}",
        "email":                email,
        "phone":                f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "date_of_birth":        str(dob),
        "gender":               gender,
        "city":                 loc[0],
        "state_province":       loc[1],
        "country":              loc[2],
        "region":               loc[3],
        "postal_code":          loc[4],
        "segment":              random.choices(SEGMENTS, SEGMENT_WEIGHTS)[0],
        "annual_income_band":   random.choices(INCOME_BANDS, INCOME_WEIGHTS)[0],
        "loyalty_tier":         tier,
        "loyalty_points":       random.randint(*pts_range),
        "acquisition_channel":  random.choices(ACQ_CHANNELS, ACQ_WEIGHTS)[0],
        "preferred_payment":    random.choice(PAYMENT_METHODS),
        "registration_date":    str(reg_date),
        "is_active":            random.random() > 0.08,
        "marketing_consent":    random.random() > 0.25,
        "_created_at":          datetime.combine(reg_date, datetime.min.time()).isoformat(),
        "_updated_at":          datetime.now().isoformat(),
    })

customers_pdf = pd.DataFrame(rows)
(spark.createDataFrame(customers_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{DIM}.dim_customer"))

spark.sql(f"ALTER TABLE {DIM}.dim_customer CLUSTER BY (country, loyalty_tier)")
print(f"dim_customer: {len(rows):,} rows")

# COMMAND ----------

# MAGIC %md ## dim_product

# COMMAND ----------

PRODUCTS = {
    "Electronics": {
        "subcategories": {
            "Smartphones":   [("iPhone 15 Pro", "Apple", 1199.00), ("Galaxy S24 Ultra", "Samsung", 1299.00),
                               ("Pixel 8 Pro", "Google", 999.00), ("OnePlus 12", "OnePlus", 799.00),
                               ("Motorola Edge 50", "Motorola", 649.00), ("Xperia 1 VI", "Sony", 1099.00)],
            "Laptops":       [("MacBook Pro 14\"", "Apple", 1999.00), ("XPS 15", "Dell", 1799.00),
                               ("Spectre x360", "HP", 1649.00), ("ThinkPad X1 Carbon", "Lenovo", 1549.00),
                               ("ROG Zephyrus G14", "Asus", 1449.00), ("Surface Laptop 5", "Microsoft", 1299.00)],
            "Audio":         [("WH-1000XM5", "Sony", 349.00), ("QuietComfort 45", "Bose", 329.00),
                               ("AirPods Pro 2", "Apple", 249.00), ("Momentum 4", "Sennheiser", 299.00),
                               ("Soundcore Q45", "Anker", 79.99), ("Jabra Evolve2 75", "Jabra", 449.00)],
            "Smart Home":    [("Echo Show 10", "Amazon", 249.99), ("Nest Hub Max", "Google", 229.99),
                               ("Ring Video Doorbell 4", "Ring", 219.99), ("Hue Starter Kit", "Philips", 199.99),
                               ("Nest Learning Thermostat", "Google", 249.99)],
            "Gaming":        [("PlayStation 5", "Sony", 499.99), ("Xbox Series X", "Microsoft", 499.99),
                               ("Nintendo Switch OLED", "Nintendo", 349.99), ("Steam Deck 512GB", "Valve", 449.00),
                               ("Meta Quest 3", "Meta", 499.99)],
        },
        "price_multiplier": 1.0,
        "cost_ratio": 0.65,
    },
    "Clothing": {
        "subcategories": {
            "Men's Tops":    [("Oxford Slim-Fit Shirt", "UrbanPulse", 79.00), ("Merino Polo", "ClassicWear", 89.00),
                               ("Linen Casual Shirt", "StyleCraft", 59.00), ("Performance Tee", "FitForge", 45.00),
                               ("Flannel Shirt", "NovaTrend", 65.00)],
            "Women's Tops":  [("Silk Wrap Blouse", "StyleCraft", 95.00), ("Cashmere Turtleneck", "ClassicWear", 145.00),
                               ("Linen Button-Down", "UrbanPulse", 75.00), ("Floral Midi Dress", "NovaTrend", 125.00),
                               ("Blazer Double-Breasted", "StyleCraft", 185.00)],
            "Denim":         [("Slim Straight Jeans", "UrbanPulse", 89.00), ("High-Rise Skinny", "NovaTrend", 95.00),
                               ("Relaxed Fit Jeans", "ClassicWear", 79.00), ("Wide Leg Jeans", "StyleCraft", 99.00)],
            "Outerwear":     [("Down Puffer Jacket", "FitForge", 219.00), ("Trench Coat", "ClassicWear", 295.00),
                               ("Wool Overcoat", "StyleCraft", 349.00), ("Rain Jacket", "UrbanPulse", 165.00)],
            "Footwear":      [("Leather Chelsea Boots", "ClassicWear", 189.00), ("Running Shoes", "FitForge", 145.00),
                               ("Canvas Sneakers", "UrbanPulse", 79.00), ("Suede Loafers", "StyleCraft", 165.00),
                               ("Hiking Boots", "NovaTrend", 195.00)],
        },
        "price_multiplier": 1.0,
        "cost_ratio": 0.40,
    },
    "Food & Beverage": {
        "subcategories": {
            "Coffee & Tea":  [("Single Origin Arabica", "PureHarvest", 24.99), ("Matcha Ceremonial Grade", "TasteFirst", 34.99),
                               ("Organic Earl Grey", "OrganiChoice", 14.99), ("Cold Brew Concentrate", "GourmetBites", 18.99),
                               ("Espresso Roast 500g", "PureHarvest", 29.99)],
            "Snacks":        [("Organic Trail Mix 500g", "NatureFresh", 12.99), ("Dark Chocolate 85%", "GourmetBites", 6.99),
                               ("Rice Crackers Multipack", "TasteFirst", 8.99), ("Mixed Nuts Premium", "PureHarvest", 19.99),
                               ("Granola Honey Oat", "OrganiChoice", 11.99)],
            "Gourmet":       [("Manuka Honey 500g", "NatureFresh", 44.99), ("White Truffle Oil 100ml", "GourmetBites", 34.99),
                               ("Hot Sauce Collection", "TasteFirst", 29.99), ("Aged Balsamic 250ml", "GourmetBites", 39.99),
                               ("Smoked Sea Salt Set", "OrganiChoice", 22.99)],
            "Beverages":     [("Sparkling Water 24pk", "NatureFresh", 29.99), ("Coconut Water 12pk", "TasteFirst", 24.99),
                               ("Vitamin C Immunity Shots", "OrganiChoice", 19.99), ("Kombucha Variety", "NatureFresh", 34.99)],
        },
        "price_multiplier": 1.0,
        "cost_ratio": 0.55,
    },
    "Home & Garden": {
        "subcategories": {
            "Kitchen":       [("Dutch Oven 5.5qt", "KitchenCraft", 149.00), ("Chef's Knife Set", "KitchenCraft", 189.00),
                               ("Espresso Machine", "HomeHaven", 449.00), ("Air Fryer XL", "KitchenCraft", 129.00),
                               ("Stand Mixer", "HomeHaven", 349.00), ("Blender Professional", "KitchenCraft", 179.00)],
            "Bedding":       [("Egyptian Cotton Sheets", "ComfortNest", 129.00), ("Down Comforter King", "ComfortNest", 199.00),
                               ("Memory Foam Pillow Pair", "ComfortNest", 89.00), ("Weighted Blanket 15lb", "DreamSpace", 99.00)],
            "Furniture":     [("Ergonomic Office Chair", "DreamSpace", 449.00), ("Solid Oak Side Table", "HomeHaven", 249.00),
                               ("Bookshelf 5-Tier", "HomeHaven", 179.00), ("TV Stand Industrial", "DreamSpace", 329.00)],
            "Garden":        [("Raised Garden Bed Cedar", "GardenBliss", 149.00), ("Compost Bin 80L", "GardenBliss", 69.00),
                               ("Garden Tool Set 5pc", "GardenBliss", 89.00), ("Outdoor String Lights 10m", "HomeHaven", 49.99)],
        },
        "price_multiplier": 1.0,
        "cost_ratio": 0.50,
    },
    "Sports & Outdoors": {
        "subcategories": {
            "Fitness":       [("Adjustable Dumbbell Set", "FitPeak", 299.00), ("Yoga Mat Premium", "ActiveEdge", 79.00),
                               ("Resistance Band Kit", "FitPeak", 49.00), ("Foam Roller Deep Tissue", "ActiveEdge", 39.00),
                               ("Pull-Up Bar Doorframe", "SportsPro", 59.00), ("Jump Rope Speed", "ActiveEdge", 29.99)],
            "Outdoor Gear":  [("Hiking Backpack 50L", "OutdoorKing", 189.00), ("2-Person Tent Ultralight", "OutdoorKing", 349.00),
                               ("Sleeping Bag -10C", "OutdoorKing", 189.00), ("Trekking Poles Carbon", "SportsPro", 139.00),
                               ("Headlamp 500 Lumen", "OutdoorKing", 59.00)],
            "Cycling":       [("Road Helmet MIPS", "SwiftStride", 129.00), ("Cycling Jersey Pro", "SwiftStride", 89.00),
                               ("Bike Computer GPS", "ActiveEdge", 249.00), ("Bike Lock U-Bar", "SportsPro", 59.00)],
            "Water Sports":  [("Inflatable SUP Board", "OutdoorKing", 649.00), ("Life Jacket Adult", "OutdoorKing", 89.00),
                               ("Dry Bag 20L", "SportsPro", 45.00), ("Snorkel Set Premium", "ActiveEdge", 79.00)],
        },
        "price_multiplier": 1.0,
        "cost_ratio": 0.48,
    },
}

product_rows = []
product_key  = 1
supplier_map = {"Apple": "SUP-001", "Samsung": "SUP-002", "Google": "SUP-003",
                "UrbanPulse": "SUP-004", "StyleCraft": "SUP-005", "FitForge": "SUP-006",
                "NatureFresh": "SUP-007", "GourmetBites": "SUP-008", "KitchenCraft": "SUP-009",
                "HomeHaven": "SUP-010", "FitPeak": "SUP-011", "OutdoorKing": "SUP-012"}

for cat, cat_data in PRODUCTS.items():
    for subcat, items in cat_data["subcategories"].items():
        for pname, brand, price in items:
            cost = round(price * cat_data["cost_ratio"] + random.uniform(-5, 5), 2)
            launch = rand_date(2019, 2023)
            product_rows.append({
                "product_key":       product_key,
                "product_id":        f"PROD-{product_key:03d}",
                "product_name":      pname,
                "brand":             brand,
                "category":          cat,
                "sub_category":      subcat,
                "sku":               f"SKU-{cat[:3].upper()}-{product_key:04d}",
                "description":       f"{pname} by {brand}. High-quality {subcat.lower()} product.",
                "unit_price":        price,
                "unit_cost":         max(cost, 1.0),
                "gross_margin_pct":  round((price - max(cost, 1.0)) / price * 100, 2),
                "weight_kg":         round(random.uniform(0.1, 15.0), 3),
                "is_active":         random.random() > 0.05,
                "launch_date":       str(launch),
                "discontinue_date":  None,
                "supplier_id":       supplier_map.get(brand, f"SUP-{random.randint(1,15):03d}"),
                "reorder_point":     random.randint(10, 100),
                "reorder_quantity":  random.randint(50, 500),
                "_created_at":       datetime.combine(launch, datetime.min.time()).isoformat(),
                "_updated_at":       datetime.now().isoformat(),
            })
            product_key += 1

products_pdf = pd.DataFrame(product_rows)
(spark.createDataFrame(products_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{DIM}.dim_product"))

spark.sql(f"ALTER TABLE {DIM}.dim_product CLUSTER BY (category, brand)")
print(f"dim_product: {len(product_rows):,} rows")

# COMMAND ----------

# MAGIC %md ## dim_store

# COMMAND ----------

STORES = [
    (1,  "New York Flagship",     "Flagship", "350 5th Avenue",        "New York",       "NY",           "United States", "Northeast", 40.748817, -73.985428,  "2015-03-15", 150, 42000, 8500000.0),
    (2,  "Los Angeles Sunset",    "Standard", "8000 Sunset Blvd",      "Los Angeles",    "CA",           "United States", "West",      34.098070, -118.366340, "2016-06-01", 85,  22000, 5200000.0),
    (3,  "Chicago Michigan Ave",  "Flagship", "600 N Michigan Ave",    "Chicago",        "IL",           "United States", "Midwest",   41.892340, -87.624780,  "2014-09-20", 120, 35000, 7100000.0),
    (4,  "Houston Galleria",      "Standard", "5085 Westheimer Rd",    "Houston",        "TX",           "United States", "South",     29.739200, -95.463400,  "2017-04-10", 75,  18000, 4300000.0),
    (5,  "Phoenix Desert Ridge",  "Standard", "21001 N Tatum Blvd",    "Phoenix",        "AZ",           "United States", "West",      33.693400, -111.981200, "2018-01-22", 60,  15000, 3800000.0),
    (6,  "Seattle Capitol Hill",  "Standard", "401 Broadway E",        "Seattle",        "WA",           "United States", "West",      47.620900, -122.320700, "2017-08-15", 70,  17500, 4100000.0),
    (7,  "Denver Cherry Creek",   "Standard", "3000 E 1st Ave",        "Denver",         "CO",           "United States", "West",      39.710400, -104.950200, "2019-02-28", 55,  14000, 3500000.0),
    (8,  "Austin Domain",         "Express",  "11410 Century Oaks Ter","Austin",         "TX",           "United States", "South",     30.401200, -97.721200,  "2020-07-01", 40,  9000,  2100000.0),
    (9,  "Nashville Broadway",    "Express",  "301 Broadway",          "Nashville",      "TN",           "United States", "South",     36.161900, -86.778700,  "2021-03-15", 35,  8500,  1900000.0),
    (10, "Charlotte SouthPark",   "Standard", "4400 Sharon Rd",        "Charlotte",      "NC",           "United States", "South",     35.155900, -80.831200,  "2018-11-01", 65,  16000, 3900000.0),
    (11, "London Oxford Street",  "Flagship", "300 Oxford St",         "London",         "England",      "United Kingdom","International", 51.514400, -0.143000, "2016-04-20", 130, 38000, 9200000.0),
    (12, "Toronto Eaton Centre",  "Standard", "220 Yonge St",          "Toronto",        "Ontario",      "Canada",        "International", 43.654700, -79.380800, "2017-10-10", 80,  20000, 5600000.0),
    (13, "Sydney Pitt Street",    "Standard", "188 Pitt St",           "Sydney",         "NSW",          "Australia",     "International", -33.867700, 151.207300, "2018-06-01", 75, 19000, 5100000.0),
    (14, "Manchester Arndale",    "Express",  "New Cathedral St",      "Manchester",     "England",      "United Kingdom","International", 53.484300, -2.238700, "2020-01-15", 45,  11000, 2800000.0),
    (15, "Online Store",          "Online",   "N/A",                   "San Francisco",  "CA",           "United States", "Online",    37.774900, -122.419400, "2013-01-01", 200, 0,     22000000.0),
]

store_rows = []
for s in STORES:
    open_dt = date.fromisoformat(s[10])
    store_rows.append({
        "store_key":       s[0],
        "store_id":        f"STORE-{s[0]:03d}",
        "store_name":      s[1],
        "store_type":      s[2],
        "address_line1":   s[3],
        "city":            s[4],
        "state_province":  s[5],
        "country":         s[6],
        "region":          s[7],
        "postal_code":     f"{random.randint(10000,99999)}",
        "latitude":        s[8],
        "longitude":       s[9],
        "open_date":       str(open_dt),
        "close_date":      None,
        "is_active":       True,
        "floor_area_sqft": s[12],
        "num_employees":   s[11],
        "annual_target":   s[13],
        "manager_id":      f"EMP-{random.randint(1,50):03d}",
        "_created_at":     datetime.combine(open_dt, datetime.min.time()).isoformat(),
        "_updated_at":     datetime.now().isoformat(),
    })

stores_pdf = pd.DataFrame(store_rows)
(spark.createDataFrame(stores_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{DIM}.dim_store"))

print(f"dim_store: {len(store_rows):,} rows")

# COMMAND ----------

# MAGIC %md ## dim_employee

# COMMAND ----------

DEPARTMENTS = ["Sales", "Sales", "Sales", "Operations", "Operations", "Finance", "HR", "IT", "Customer Service"]
JOB_TITLES = {
    "Sales":            ["Sales Associate", "Senior Sales Associate", "Store Manager", "Assistant Manager"],
    "Operations":       ["Stock Controller", "Warehouse Operative", "Logistics Coordinator"],
    "Finance":          ["Finance Analyst", "Senior Accountant"],
    "HR":               ["HR Business Partner", "Talent Acquisition Specialist"],
    "IT":               ["Systems Administrator", "Data Analyst"],
    "Customer Service": ["Customer Service Rep", "Customer Experience Manager"],
}
SALARY_BANDS = {
    "Sales Associate": "B2", "Senior Sales Associate": "B3", "Store Manager": "M2",
    "Assistant Manager": "M1", "Stock Controller": "B2", "Warehouse Operative": "B1",
    "Logistics Coordinator": "B3", "Finance Analyst": "B3", "Senior Accountant": "M1",
    "HR Business Partner": "B3", "Talent Acquisition Specialist": "B3",
    "Systems Administrator": "B3", "Data Analyst": "B3",
    "Customer Service Rep": "B2", "Customer Experience Manager": "M1",
}

employee_rows = []
store_keys = [s["store_key"] for s in store_rows if s["store_type"] != "Online"]

for i in range(1, 51):
    dept      = random.choice(DEPARTMENTS)
    title     = random.choice(JOB_TITLES[dept])
    store_key = random.choice(store_keys)
    hire_dt   = rand_date(2015, 2023)
    is_active = random.random() > 0.10
    mgr_key   = random.randint(1, 10) if i > 10 else None

    employee_rows.append({
        "employee_key":    i,
        "employee_id":     f"EMP-{i:03d}",
        "first_name":      random.choice(FIRST_NAMES),
        "last_name":       random.choice(LAST_NAMES),
        "email":           f"emp{i:03d}@northwindanalytics.com",
        "phone":           f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "department":      dept,
        "job_title":       title,
        "salary_band":     SALARY_BANDS.get(title, "B2"),
        "hire_date":       str(hire_dt),
        "termination_date": None if is_active else str(rand_date(2023, 2024)),
        "store_key":       store_key,
        "manager_key":     mgr_key,
        "performance_rating": round(random.uniform(2.5, 5.0), 1),
        "is_active":       is_active,
        "_created_at":     datetime.combine(hire_dt, datetime.min.time()).isoformat(),
        "_updated_at":     datetime.now().isoformat(),
    })

employees_pdf = pd.DataFrame(employee_rows)
(spark.createDataFrame(employees_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{DIM}.dim_employee"))

print(f"dim_employee: {len(employee_rows):,} rows")

# COMMAND ----------

# MAGIC %md ## dim_promotion

# COMMAND ----------

PROMOS = [
    ("PROMO-001", "Summer Splash Sale",         "Percentage", 0.20, "2023-06-01", "2023-08-31", "Clothing",          0.0,    True),
    ("PROMO-002", "Black Friday Mega Deal",      "Percentage", 0.30, "2023-11-24", "2023-11-26", None,               50.0,   False),
    ("PROMO-003", "Tech Tuesday",                "Percentage", 0.15, "2023-01-01", "2023-12-31", "Electronics",       0.0,    False),
    ("PROMO-004", "Buy 2 Get 1 Free",            "BOGO",       0.33, "2023-03-01", "2023-03-31", "Food & Beverage",   0.0,    False),
    ("PROMO-005", "New Year New Gear",           "Percentage", 0.25, "2024-01-01", "2024-01-14", "Sports & Outdoors", 0.0,    False),
    ("PROMO-006", "Spring Refresh",              "Percentage", 0.15, "2024-03-20", "2024-04-20", "Home & Garden",     0.0,    True),
    ("PROMO-007", "Back to School Bundle",       "Bundle",     0.12, "2023-08-01", "2023-09-15", "Electronics",      100.0,  False),
    ("PROMO-008", "Loyalty Gold Reward",         "Fixed",      0.10, "2023-01-01", "2023-12-31", None,                0.0,   True),
    ("PROMO-009", "Cyber Monday Blowout",        "Percentage", 0.35, "2023-11-27", "2023-11-27", None,               25.0,   False),
    ("PROMO-010", "Valentine's Day Gifts",       "Percentage", 0.18, "2024-02-07", "2024-02-14", "Clothing",          0.0,   False),
    ("PROMO-011", "Earth Day Green Sale",        "Percentage", 0.12, "2024-04-22", "2024-04-22", "Food & Beverage",   0.0,   True),
    ("PROMO-012", "Father's Day Special",        "Percentage", 0.20, "2024-06-10", "2024-06-16", "Sports & Outdoors", 0.0,  False),
    ("PROMO-013", "Prime Competitor Event",      "Percentage", 0.22, "2023-07-11", "2023-07-13", None,                0.0,   False),
    ("PROMO-014", "Holiday Season Savings",      "Percentage", 0.25, "2023-12-01", "2023-12-24", None,               75.0,   False),
    ("PROMO-015", "Black Friday 2024",           "Percentage", 0.28, "2024-11-29", "2024-12-01", None,               30.0,   False),
    ("PROMO-016", "Cyber Monday 2024",           "Percentage", 0.32, "2024-12-02", "2024-12-02", None,               25.0,   False),
    ("PROMO-017", "Winter Clearance",            "Percentage", 0.40, "2024-01-15", "2024-02-28", "Clothing",          0.0,   False),
    ("PROMO-018", "Home Office Upgrade",         "Bundle",     0.15, "2023-09-01", "2023-10-31", "Electronics",      150.0,  True),
    ("PROMO-019", "Fitness New Year 2024",       "Percentage", 0.20, "2024-01-01", "2024-01-31", "Sports & Outdoors", 0.0,  False),
    ("PROMO-020", "Loyalty Platinum Exclusive",  "Percentage", 0.15, "2023-01-01", "2024-12-31", None,                0.0,   True),
]

promo_rows = []
for idx, p in enumerate(PROMOS, 1):
    promo_rows.append({
        "promotion_key":         idx,
        "promotion_id":          p[0],
        "promotion_name":        p[1],
        "promotion_type":        p[2],
        "discount_rate":         p[3],
        "start_date":            p[4],
        "end_date":              p[5],
        "applicable_category":   p[6],
        "min_order_value":       p[7],
        "is_stackable":          p[8],
        "_created_at":           datetime.now().isoformat(),
    })

promos_pdf = pd.DataFrame(promo_rows)
(spark.createDataFrame(promos_pdf)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{DIM}.dim_promotion"))

print(f"dim_promotion: {len(promo_rows):,} rows")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

for tbl in ["dim_date", "dim_customer", "dim_product", "dim_store", "dim_employee", "dim_promotion"]:
    cnt = spark.table(f"{DIM}.{tbl}").count()
    print(f"  {DIM}.{tbl}: {cnt:,} rows")
print("\nAll dimension tables created successfully.")
