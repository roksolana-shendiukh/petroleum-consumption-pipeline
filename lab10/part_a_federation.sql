-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Ingest tables into Delta

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.roksolana_shendiu770_lab10

-- COMMAND ----------

CREATE OR REPLACE TABLE dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta AS
SELECT ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight,
       ProductCategoryID, ProductModelID, SellStartDate, SellEndDate,
       DiscontinuedDate, ModifiedDate
FROM azure_sql_lab10_conn_catalog.saleslt.product

-- COMMAND ----------

CREATE OR REPLACE TABLE dbr_dev_ua5816bd.roksolana_shendiu770_lab10.productcategory_delta AS
SELECT * FROM azure_sql_lab10_conn_catalog.saleslt.productcategory

-- COMMAND ----------

CREATE OR REPLACE TABLE dbr_dev_ua5816bd.roksolana_shendiu770_lab10.salesorderdetail_delta AS
SELECT * FROM azure_sql_lab10_conn_catalog.saleslt.salesorderdetail

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Federation vs Delta: the same query

-- COMMAND ----------

SELECT p.Name, SUM(d.LineTotal) AS revenue
FROM azure_sql_lab10_conn_catalog.saleslt.salesorderdetail d
JOIN azure_sql_lab10_conn_catalog.saleslt.product p ON p.ProductID = d.ProductID
GROUP BY p.Name
ORDER BY revenue DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![l.png](./l.png "l.png")

-- COMMAND ----------

SELECT p.Name, SUM(d.LineTotal) AS revenue
FROM dbr_dev_ua5816bd.roksolana_shendiu770_lab10.salesorderdetail_delta d
JOIN dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta p ON p.ProductID = d.ProductID
GROUP BY p.Name
ORDER BY revenue DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![Screenshot 2026-10-07 124747.png](./Screenshot 2026-10-07 124747.png "Screenshot 2026-10-07 124747.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Pushdown

-- COMMAND ----------

EXPLAIN FORMATTED
SELECT ProductID, Name
FROM azure_sql_lab10_conn_catalog.saleslt.product
WHERE ListPrice > 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![pushdown.png](./pushdown.png "pushdown.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Join an external table with a Delta table

-- COMMAND ----------

SELECT c.Name AS category, COUNT(*) AS products, ROUND(AVG(p.ListPrice), 2) AS avg_price
FROM azure_sql_lab10_conn_catalog.saleslt.product p
JOIN dbr_dev_ua5816bd.roksolana_shendiu770_lab10.productcategory_delta c
  ON c.ProductCategoryID = p.ProductCategoryID
GROUP BY c.Name
ORDER BY products DESC
LIMIT 10