# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Generate views - Use Autoloader

# COMMAND ----------

import dlt

# COMMAND ----------

# The following values should be set in the dlt pipeline's settings under configurations:
volume_name = spark.conf.get("volume_name")
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")

your_volume = f"{volume_name}"
your_schema = f"{catalog_name}.{schema_name}"

data_sources = [
    {"view_name": f"{your_schema}.orders_raw_view", "source": f"{your_volume}/raw/orders/"},
    {"view_name": f"{your_schema}.products_raw_view", "source": f"{your_volume}/raw/products/"},
    {"view_name": f"{your_schema}.customers_raw_view", "source": f"{your_volume}/raw/customers/"},
]



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

product_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

def create_raw_tables(view_name, source):  
    @dlt.table(
        name=view_name,
        table_properties={"quality": "raw"},
        comment=f"Parsed streaming data for silver {view_name} records"
    )
    def create_raw_table():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(product_schema)   
            .load(source)
            # .dropDuplicates(["id"])   
        )


# COMMAND ----------

# Orders Data

# orders_schema_location = "/mnt/data/orders/schema"

def create_raw_tables(view_name, source):  
    @dlt.table(
        name=view_name,
        table_properties={"quality": "raw"},
        comment=f"Parsed streaming data for silver {view_name} records"
    )
    def create_raw_table():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(source)
            .dropDuplicates(["id"])
            )

# COMMAND ----------

# Loop through each data source and create the corresponding view
for data_source in data_sources:
    view_name = data_source["view_name"]
    source = data_source["source"]

    # Create the view
    create_raw_tables(view_name, source)
