# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Generate views from json data

# COMMAND ----------

import dlt

# COMMAND ----------

root ='abfss://unity-catalog-storage@dbstoragefkr2wabue4hpg.dfs.core.windows.net/2523655398384705'

data_sources = [
    {"view_name": "orders_raw_view", "source": f"{root}/raw/orders/"},
    {"view_name": "products_raw_view", "source": f"{root}/raw/products/"},
    {"view_name": "customers_raw_view", "source": f"{root}/raw/customers/"},
]

# Adding this comment to test 

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
            .option("cloudFiles.inferColumnTypes", "true")
            # .option("cloudFiles.schemaLocation", orders_schema_location)
            .load(source)
            )

# COMMAND ----------

# Loop through each data source and create the corresponding view
for data_source in data_sources:
    view_name = data_source["view_name"]
    source = data_source["source"]

    # Create the view
    create_raw_tables(view_name, source)
