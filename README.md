# Apple Data Analysis using PySpark

## Project Overview

This project involves creating an ETL pipeline to analyze customer transactions related to Apple products using Apache Spark in the Databricks environment. The goal is to extract, transform, and load (ETL) data to derive insights into customer purchasing patterns.

## Key Features

- **ETL Pipelines**: Implemented pipelines to address specific business logic questions regarding Apple product purchases.
- **Factory Design Pattern**: Used to create flexible and reusable extractors, transformers, and loaders.
- **Data Processing**: Involved handling CSV, Parquet, and Delta file formats using Spark.
- **Data Analysis**: Focused on key metrics such as customer purchase sequences, average time delays between purchases, and top-selling products by revenue.

## Technologies Used

- Apache Spark with PySpark
- Databricks environment
- Delta Lake

## Setup and Installation

1. **Databricks Setup**:
   - Create a cluster in Databricks.
   - Upload the required datasets (`customer.csv`, `products.csv`, `transactions.csv`) to DBFS.
   - Import the provided `.py` files as notebooks.

2. **Running the Project**:
   - Ensure all notebooks are uploaded and available in Databricks.
   - Run the main analysis notebook (`AppleAnalysisMainCode.py`) to execute the ETL pipelines.
   - Results will be stored in DBFS or Delta tables as per the defined loaders.


## Datasets

The project uses the following datasets:
- `customer.csv`: Contains customer information.
- `products.csv`: Contains product details.
- `transactions.csv`: Contains transaction records.

