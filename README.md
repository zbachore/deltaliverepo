# deltaliverepo
Delta Live code from databricks  will live here.

# Delta Live Tables

This repository demonstrates the usage of Delta Live Tables (DLT) using a configuration-driven method. The notebooks are designed to be simple and easy to understand, showcasing how to generate, process, and manage data using Delta Live Tables in Databricks.

## Notebooks

### 1. Generate Sample Data - Notebook

This notebook generates sample data for orders, customers, and products, and saves them as JSON files in your Databricks workspace. It also creates temporary views for each dataset.

- **Orders Data**: Generates 100 records with columns such as `order_id`, `customer_id`, `product_id`, `quantity`, `price`, `order_status`, and `timestamp`.
- **Customers Data**: Generates 1000 unique customers with columns such as `customer_id`, `customer_name`, `email`, `phone_number`, `address`, and `timestamp`.
- **Products Data**: Generates 100 unique products with columns such as `product_id`, `product_name`, `category`, `price`, and `brand`.

### 2. Generate Raw Views - Notebook

This notebook sets up Delta Live Tables (DLT) to create views from JSON data stored in Azure storage. It reads streaming data and creates raw tables for orders, products, and customers using autoloader.

  **root**: this is the root location of your catalog
- **data_sources**: json structure for your data sources configuration.
- **create_raw_tables**: function using the `dlt.table` decorator to create Delta Live Tables with specified properties.

### 3. Load Bronze - Notebook

This notebook sets up Delta Live Tables (DLT) to create bronze tables from raw data views. It applies data quality expectations and transformations to ensure the data meets specified criteria before being stored in the bronze tables.

- **bronze_tables_config**: Specifies configuration for bronze tables, including source views and columns to select. 
- **data_quality_expectations**: Defines data quality rules for each bronze table.
- **create_bronze_tables**: Uses the `dlt.table` decorator to create Delta Live Tables with specified properties and data quality rules.

### 4. Load Silver - Notebook

This notebook sets up Delta Live Tables (DLT) to create silver tables from bronze tables. It applies data quality expectations and transformations to ensure the data meets specified criteria before being stored in the silver tables.

- **silver_tables_config**: Specifies configuration for silver tables, including source tables, columns to select, partition columns, and comments.
- **data_quality_expectations**: Defines data quality rules for each silver table.
- **create_silver_tables**: Function definition using the `dlt.table` decorator to create Delta Live Tables with specified properties and data quality rules.

### 5. Gold View - Notebook

This notebook creates a gold view by joining the silver tables for orders, customers, and products. It filters out orders with a status of 'Pending' and selects relevant columns to create a comprehensive view of the data.

- **Create or Refresh Streaming Table**: Creates or refreshes a streaming table named `gold_view`.
- **Join Silver Tables**: Performs inner joins between the `Orders_silver`, `customers_silver`, and `Products_silver` tables.
- **Filter Orders**: Filters out orders with a status of 'Pending'.
- **Select Columns**: Selects columns such as `customer_id`, `customer_name`, `order_id`, `product_id`, `quantity`, `price`, and `brand`.

## Getting Started

1. **Copy Repository URL**:
   ```bash
   https://github.com/zbachore/deltaliverepo.git

2. **Link Your GitHub Account to Your Databricks Workspace**:

- In the upper-right corner of any Databricks page, click your username and select Settings.
- Click the Linked accounts tab.
- Change your provider to GitHub.
- Select Link Git account and click Link.
- The Databricks GitHub App authorization page will appear.
- Authorize the GitHub App to complete the setup.
- That's it! Your GitHub account should now be linked to your Databricks workspace 

3. **Clone the repository into your Databricks workspace**: 
- Under the "Home" Folder, create a Git folder by clicking "Create" and selecting "Git Folder".
- Paste the repository url under "Git repository URL" and select "GitHub" under "Git provider".
- Git folder name can be any name but desirable to use the same name as the repository name.

4. **Create a pipeline**:
- Under Delta Live Tables, create a pipeline by adding all the notebooks listed above.

Conclusion
This repository provides a simple and clear example of using Delta Live Tables in Databricks to manage and process data in a configuration-driven manner. Feel free to explore and modify the notebooks to suit your needs.
