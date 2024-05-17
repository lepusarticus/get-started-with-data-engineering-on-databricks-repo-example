# Databricks notebook source
# MAGIC %md
# MAGIC ### This is my first Notebook

# COMMAND ----------

# Starting a Spark session
from pyspark.sql import SparkSession

spark = (SparkSession.builder 
    .appName("MyApp") 
    .getOrCreate()
)

# Connecting to a database
spark.sql("CREATE DATABASE IF NOT EXISTS my_database")
spark.sql("USE my_database")

# COMMAND ----------

# Creating a table with sales transactions
spark.sql("""
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DOUBLE,
    transaction_date DATE
)
""")

# Populating the table with sample data
spark.sql("""
INSERT INTO sales_transactions
VALUES
    (1, 101, 201, 2, 10.99, '2021-01-01'),
    (2, 102, 202, 1, 5.99, '2021-01-02'),
    (3, 103, 203, 3, 15.99, '2021-01-03'),
    (4, 104, 204, 2, 9.99, '2021-01-04'),
    (5, 105, 205, 1, 7.99, '2021-01-05')
""")

# COMMAND ----------



# COMMAND ----------


