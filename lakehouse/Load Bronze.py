# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Load Bronze from json raw

# COMMAND ----------

import dlt

# COMMAND ----------

bronze_tables_config = {
    "orders_bronze": {
        "source_view": "orders_raw_view",
        "select_columns": ["*"],
    },
    "customers_bronze": {
        "source_view": "customers_raw_view",
        "select_columns": ["*"]
    },

    "products_bronze": {
        "source_view": "products_raw_view",
        "select_columns": ["*"]
    },
} 

data_quality_expectations = {
    "orders_bronze": {
        "expect_or_drop": {
            "rules": {
                "valid_order_id": "order_id IS NOT NULL",
                "valid_customer_id": "valid_customer_id IS NOT NULL",
                "valid_price": "price >= 0"
            }
        },
        "expect_or_quarantine": {
            "rules": {
                "quarantine_rule": "_rescued_data IS NOT NULL OR order_id IS NULL OR price IS NULL OR price < 0"
            }
        }
    },
    "customers_bronze": {
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

    "products_bronze": {
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

# Define bronze logic function
from pyspark.sql.functions import year, month

def create_bronze_tables(table_name, source_view, select_columns, drop_rule, quarantine_rule):  
    if not drop_rule:
        drop_rule = {"default_rule": "1 == 1"}
    @dlt.table(
        name=table_name,
        table_properties={"quality": "bronze"},
        comment=f"Parsed streaming data for bronze {table_name} records"
    )
    @dlt.expect_all_or_drop(drop_rule)
    # @dlt.expect_or_quarantine(quarantine_rule)                    
    def create_bronze_table():
        return (
            dlt.read_stream(source_view)
              .select(*select_columns)
        )

# COMMAND ----------

expect_or_drop_rule = None
expect_or_quarantine_rule = None

for table, config in bronze_tables_config.items():
    # Reset rules for each table iteration
    expect_or_drop_rule = None
    expect_or_quarantine_rule = None

    # Iterate over the data quality expectations
    for rule_name, rule_details in data_quality_expectations.items():
        if rule_details.get("target_table_name") == table:
            # Retrieve the rules based on the rule name
            if rule_name == "expect_or_drop":
                expect_or_drop_rule = rule_details.get("rules")
            if rule_name == "expect_or_quarantine":
                expect_or_quarantine_rule = rule_details.get("rules")

    # Call the function with the retrieved rules
    create_bronze_tables(table, config["source_view"], config["select_columns"], expect_or_drop_rule, expect_or_quarantine_rule)

