# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Generate Orders Data

# COMMAND ----------

from pyspark.sql.functions import col, expr, pandas_udf, split, concat_ws, lit, lower
from pyspark.sql.functions import rand, round, current_timestamp
from datetime import datetime
import uuid
import json
import random
import pandas as pd

# COMMAND ----------

# The volume_name value should be set in the dlt pipeline settings under configurations:
volume_name = spark.conf.get("volume_name")
your_volume = f"{volume_name}"

# The value of num_records should be set in the dlt pipeline settings under configurations:
# This is just to allow me to change the number of records to create right from the dlt pipeline itself without opening the notebook
num_records = int(spark.conf.get("num_records"))

# Number of records to generate
num_records = num_records # for orders
num_customers = num_records # for customers
num_products = num_records # for products



# COMMAND ----------

# Get the current timestamp for the filename
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Generate the sample data
df = (
    spark.range(num_records)
    .withColumn("order_id", expr("uuid()"))
    .withColumn("customer_id", (col("id") % 1000).cast("int"))
    .withColumn("product_id", (col("id") % 100).cast("int"))
    .withColumn("quantity", round(1 + rand() * 9).cast("int"))
    .withColumn("price", round(10 + rand() * 490, 2))
    .withColumn("order_status", expr("CASE WHEN rand() > 0.1 THEN 'Completed' ELSE 'Pending' END"))
    .withColumn("timestamp", current_timestamp())
)

# Show the sample data
# df.show(truncate=False)

# Save the DataFrame as a JSON file in your Databricks workspace with the timestamp in the filename
output_path = f"{your_volume}/raw/orders/orders_data_{timestamp}.json"
df.write.mode("append").json(output_path)

print(f"orders data saved to {output_path}")

# df.createOrReplaceTempView('Orders')




# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Generate Customers Data

# COMMAND ----------


# Get the current timestamp for the filename
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# List of common first and last names
first_names = ["John", "Jane", "Michael", "Emily", "David", "Emma", "Chris", "Olivia", "Daniel", "Sophia"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

# Lists of street names, cities, and ZIP codes
street_names = ["Maple St", "Oak St", "Pine St", "Cedar St", "Elm St", "Washington Ave", "2nd Ave", "3rd St", "4th St", "5th Ave"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
zip_codes = ["10001", "90001", "60601", "77001", "85001", "19101", "78201", "92101", "75201", "95101"]

# Function to generate a random customer name
def random_customer_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Function to generate a random address
def random_address():
    street_number = random.randint(100, 9999)
    return f"{street_number} {random.choice(street_names)}, {random.choice(cities)}, {random.choice(zip_codes)}"

# Pandas UDF to apply the random_customer_name function to a DataFrame
@pandas_udf("string")
def generate_customer_names(id_series: pd.Series) -> pd.Series:
    return pd.Series([random_customer_name() for _ in id_series])

# Pandas UDF to apply the random_address function to a DataFrame
@pandas_udf("string")
def generate_addresses(id_series: pd.Series) -> pd.Series:
    return pd.Series([random_address() for _ in id_series])

# Function to generate random phone numbers
def random_phone_number():
    return f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

# Generate the customer data
customer_df = (
    spark.range(num_customers)
    .withColumn("customer_id", col("id").cast("int"))
    .withColumn("customer_name", generate_customer_names(col("id")))
    .withColumn("first_name", split(col("customer_name"), " ").getItem(0))
    .withColumn("last_name", split(col("customer_name"), " ").getItem(1))
    .withColumn("email", concat_ws("@", concat_ws(".", lower(col("first_name")), lower(col("last_name"))), concat_ws(".", lower(col("last_name")), lit("com"))))
    .withColumn("phone_number", lit(random_phone_number()))  # For simplicity, this will be the same number
    .withColumn("address", generate_addresses(col("id")))
    .withColumn("timestamp", current_timestamp())
    .drop("first_name", "last_name")  # Remove intermediate columns
)

# Show the customer data
# customer_df.show(truncate=False)

# Save the DataFrame as a JSON file in your Databricks workspace

customer_output_path = f"{your_volume}/raw/customers/customer_data_{timestamp}.json"
customer_df.write.mode("overwrite").json(customer_output_path)

print(f"Customer data saved to {customer_output_path}")

# customer_df.createOrReplaceTempView('Customers')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate Product Data  
# MAGIC

# COMMAND ----------

# Get the current timestamp for the filename
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Lists of product names, categories, and realistic brand names
product_names = ["Laptop", "Smartphone", "Headphones", "Tablet", "Monitor", "Keyboard", "Mouse", "Printer", "Camera", "Smartwatch"]
categories = ["Electronics", "Computers", "Accessories", "Office Supplies"]
brands = ["Apple", "Samsung", "Sony", "Dell", "HP", "Logitech", "Canon", "Nikon", "Microsoft", "Bose"]

# Generate random product data in plain Python
random_product_data = []
for i in range(num_products):
    product_id = i
    id = i
    product_name = f"{random.choice(product_names)} Model {random.randint(100, 999)}"
    category = random.choice(categories)
    price = float(f"{50 + random.random() * 950:.2f}")  # Manually format price to two decimal places
    brand = random.choice(brands)
    random_product_data.append((id, product_id, product_name, category, price, brand))

# Create a Spark DataFrame from the generated product data
product_df = spark.createDataFrame(random_product_data, ["id", "product_id", "product_name", "category", "price", "brand"])
product_df = product_df.withColumn("timestamp", current_timestamp())

# Show the product data
# product_df.show(truncate=False)

# Save the DataFrame as a JSON file in your Databricks workspace
product_output_path = f"{your_volume}/raw/products/product_data_{timestamp}.json"
product_df.write.mode("overwrite").json(product_output_path)

print(f"Product data saved to {product_output_path}")

# product_df.createOrReplaceTempView('Products')

