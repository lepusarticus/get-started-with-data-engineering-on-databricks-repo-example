# Databricks notebook source
# MAGIC %md
# MAGIC ### This is my first Notebook

# COMMAND ----------

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

# Populating the table with sample data using MERGE INTO statement
spark.sql("""
MERGE INTO sales_transactions AS target
USING (
    VALUES
        (1, 101, 201, 2, 10.99, '2021-01-01'),
        (2, 102, 202, 1, 5.99, '2021-01-02'),
        (3, 103, 203, 3, 15.99, '2021-01-03'),
        (4, 104, 204, 2, 9.99, '2021-01-04'),
        (5, 105, 205, 1, 7.99, '2021-01-05'),
        (7, 105, 204, 7, 9.99, '2024-01-05')
) AS source
ON target.transaction_id = source.col1
WHEN NOT MATCHED THEN
    INSERT (transaction_id, customer_id, product_id, quantity, price, transaction_date)
    VALUES (source.col1, source.col2, source.col3, source.col4, source.col5, source.col6)
""")

# COMMAND ----------

display(spark.table('sales_transactions'))


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales_transactions
