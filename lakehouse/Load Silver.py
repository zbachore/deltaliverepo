# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Load silver from json bronze

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import year, month, day, hour, current_timestamp 
from datetime import datetime
# timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# COMMAND ----------

silver_tables_config = {
    "orders_silver": {
        "source_table": "orders_bronze",
        "select_columns": ["*", year("timestamp").alias("year"), month("timestamp").alias("month"), day("timestamp").alias("day"), hour("timestamp").alias("hour")],
        "partition_cols": ["year", "month", "day", "hour"],
        "comments": "Dropping pending orders and partitioning by year, month, day, hour"
    },
    "customers_silver": {
        "source_table": "customers_bronze",
        "select_columns": ["*", year("timestamp").alias("year"), month("timestamp").alias("month"), day("timestamp").alias("day"), hour("timestamp").alias("hour")],
        "partition_cols": ["year", "month", "day", "hour"],
        "comments":"Partitioning is not required."
    },

    "products_silver": {
        "source_table": "products_bronze",
        "select_columns": ["*"],
        "partition_cols": ["brand"],
        "comments":"Partitioned by brand name."
    }, 
} 

data_quality_expectations = {
    "orders_silver": {
      "target_table_name": "orders_silver",
        "expect_or_drop": {
            "rules": {
                "valid_order_id": "order_id IS NOT NULL",
                "valid_customer_id": "customer_id IS NOT NULL",
                "valid_price": "price >= 0",
                "valid_orders": "order_status == 'Completed'"
            }
        },
        "expect_or_quarantine": {
            "rules": {
                "quarantine_rule": "_rescued_data IS NOT NULL OR order_id IS NULL OR price IS NULL OR price < 0"
            }
        }
    },
    "customers_silver": {
      "target_table_name": "customers_silver",
        "expect_or_drop": {
            "rules": {
                "valid_customer_id": "customer_id IS NOT NULL",
                "valid_address": "address IS NOT NULL",
                "valid_email": "email IS NOT NULL"
            }
        },
        "expect_or_quarantine": {
            "rules": {
                "quarantine_rule": "_rescued_data IS NOT NULL OR customer_id IS NULL OR address IS NULL OR email IS NULL"
            }
        }
    },

    "products_silver": {
      "target_table_name": "products_silver",
        "expect_or_drop": {
            "rules": {
                "valid_product_id": "product_id IS NOT NULL",
                "valid_brand": "brand IS NOT NULL",
                "valid_price": "price >= 0"
            }
        },
        "expect_or_quarantine": {
            "rules": {
                "quarantine_rule": "_rescued_data IS NOT NULL OR product_id IS NULL OR brand IS NULL OR price < 0"
            }
        }
    },

}


# COMMAND ----------

from pyspark.sql.functions import year, month

def create_silver_tables(table_name, source_table, select_columns, drop_rule, quarantine_rule, partition_cols, comments):  
    # If drop_rule is not provided, it's set to a default that won't drop any rows
    if not drop_rule:
      drop_rule = {"default_rule": "1 == 1"}
    
    # Print the drop_rule to ensure it's correctly set
    print(f"Applying drop_rule for {table_name}: {drop_rule}")
    
    @dlt.table(
        name=table_name,
        partition_cols=partition_cols,
        table_properties={"quality": "silver"},
        comment=f"{comments}"
    )
    @dlt.expect_all_or_drop(drop_rule)
    # @dlt.expect_or_quarantine(quarantine_rule)                    
    def create_silver_table():
        return (
            dlt.read_stream(source_table)
              .select(*select_columns)
        )


# COMMAND ----------

for table, config in silver_tables_config.items():
    # Reset rules for each table iteration
    expect_or_drop_rule = None
    expect_or_quarantine_rule = None
    
    # Iterate over the data quality expectations
    for rule_name, rule_details in data_quality_expectations.items():
        if rule_details.get("target_table_name") == table:
            # Retrieve the rules based on the rule name
            expect_or_drop_rule = rule_details.get("expect_or_drop", {}).get("rules", None)
            expect_or_quarantine_rule = rule_details.get("expect_or_quarantine", {}).get("rules", None)
            
    # Call the function or print the values for verification
    # print(table, config["source_table"], config["select_columns"], expect_or_drop_rule, expect_or_quarantine_rule)


    # Call the function with the retrieved rules
    create_silver_tables(table, config["source_table"], config["select_columns"], expect_or_drop_rule, expect_or_quarantine_rule, config["partition_cols"], config["comments"])

