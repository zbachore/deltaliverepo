# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Generate Orders Data

# COMMAND ----------

# Number of records to generate
num_records = 10
num_customers = 10
num_products = 10

# COMMAND ----------

volume_name = spark.conf.get("volume_name")
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")

# COMMAND ----------

from pyspark.sql.functions import col, rand, round, expr, current_timestamp
from datetime import datetime
import uuid

catalog = catalog_name
schema = schema_name
num_records = 10  # Always insert 10 rows per run

# Generate the sample data
df = (
    spark.range(num_records)
    .withColumn("order_id", expr("uuid()"))
    .withColumn("customer_id", (col("id") % 1000).cast("int"))
    .withColumn("product_id", (col("id") % 100).cast("int"))
    .withColumn("quantity", (round(1 + rand() * 9)).cast("int"))
    .withColumn("price", (round(10 + rand() * 490, 2)))
    .withColumn("order_status", expr("CASE WHEN rand() > 0.1 THEN 'Completed' ELSE 'Pending' END"))
    .withColumn("timestamp", current_timestamp())
)

# Write the DataFrame as a Delta table
df.write.mode("append").format("delta").saveAsTable(f"{catalog}.{schema}.orders_source")

print(f"Orders data appended into {catalog}.{schema}.orders_source")

# Optional: create temp view if needed
df.createOrReplaceTempView('Orders')


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Generate Customers Data

# COMMAND ----------

from pyspark.sql.functions import col, expr, pandas_udf, split, concat_ws, lit, lower, current_timestamp
import random
import pandas as pd

num_customers = 10  # Always insert 10 rows per run

# Lists of realistic names and addresses
first_names = ["John", "Jane", "Michael", "Emily", "David", "Emma", "Chris", "Olivia", "Daniel", "Sophia"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
street_names = ["Maple St", "Oak St", "Pine St", "Cedar St", "Elm St", "Washington Ave", "2nd Ave", "3rd St", "4th St", "5th Ave"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
zip_codes = ["10001", "90001", "60601", "77001", "85001", "19101", "78201", "92101", "75201", "95101"]

# Function to generate random customer name
def random_customer_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Function to generate random address
def random_address():
    street_number = random.randint(100, 9999)
    return f"{street_number} {random.choice(street_names)}, {random.choice(cities)}, {random.choice(zip_codes)}"

# Pandas UDFs
@pandas_udf("string")
def generate_customer_names(id_series: pd.Series) -> pd.Series:
    return pd.Series([random_customer_name() for _ in id_series])

@pandas_udf("string")
def generate_addresses(id_series: pd.Series) -> pd.Series:
    return pd.Series([random_address() for _ in id_series])

# Generate customer DataFrame
customer_df = (
    spark.range(num_customers)
    .withColumn("customer_id", col("id").cast("int"))
    .withColumn("customer_name", generate_customer_names(col("id")))
    .withColumn("first_name", split(col("customer_name"), " ").getItem(0))
    .withColumn("last_name", split(col("customer_name"), " ").getItem(1))
    .withColumn("email", concat_ws("@", concat_ws(".", lower(col("first_name")), lower(col("last_name"))), lit("example.com")))
    .withColumn("phone_number", expr(f"'+1-' || lpad(cast(floor(rand() * 900) + 100 as string),3,'0') || '-' || lpad(cast(floor(rand() * 900) + 100 as string),3,'0') || '-' || lpad(cast(floor(rand() * 9000) + 1000 as string),4,'0')"))
    .withColumn("address", generate_addresses(col("id")))
    .withColumn("timestamp", current_timestamp())
    .drop("first_name", "last_name")  # Drop intermediate fields
)

# Write to Delta Table
customer_df.write.mode("append").format("delta").saveAsTable(f"{catalog}.{schema}.customers_source")

print(f"Customer data appended into {catalog}.{schema}.customers_source")

# Optional: create temp view
customer_df.createOrReplaceTempView('Customers')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate Product Data  
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
import random
from datetime import datetime

num_products = 10  # Always insert 10 rows per run

# Lists of product names, categories, and realistic brand names
product_names = ["Laptop", "Smartphone", "Headphones", "Tablet", "Monitor", "Keyboard", "Mouse", "Printer", "Camera", "Smartwatch"]
categories = ["Electronics", "Computers", "Accessories", "Office Supplies"]
brands = ["Apple", "Samsung", "Sony", "Dell", "HP", "Logitech", "Canon", "Nikon", "Microsoft", "Bose"]

# Generate random product data
random_product_data = []
for i in range(num_products):
    id_value = i
    product_id = i
    product_name = f"{random.choice(product_names)} Model {random.randint(100, 999)}"
    category = random.choice(categories)
    price = float(f"{50 + random.random() * 950:.2f}")  # Two decimal places
    brand = random.choice(brands)
    random_product_data.append((id_value, product_id, product_name, category, price, brand))

# Create Spark DataFrame
product_df = spark.createDataFrame(random_product_data, ["id", "product_id", "product_name", "category", "price", "brand"])
product_df = product_df.withColumn("timestamp", current_timestamp())

# Write to Delta Table
product_df.write.mode("append").format("delta").saveAsTable(f"{catalog}.{schema}.products_source")

print(f"Product data appended into {catalog}.{schema}.products_source")

# Optional: create temp view
product_df.createOrReplaceTempView('Products')

