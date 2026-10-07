-- Databricks notebook source
ALTER TABLE dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

DESCRIBE HISTORY dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![Screenshot 2026-10-07 131922.png](./Screenshot 2026-10-07 131922.png "Screenshot 2026-10-07 131922.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Simulate changes in the source (version 2, 3, 4)

-- COMMAND ----------

INSERT INTO dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta
VALUES (1001, 'Bike', 'JK-0001', 'Black', 500.00, 899.99, 'M', 12.50,
        6, NULL, current_timestamp(), NULL, NULL, current_timestamp());



-- COMMAND ----------

UPDATE dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta
SET ListPrice = 1500.00, ModifiedDate = current_timestamp()
WHERE ProductID = 680;



-- COMMAND ----------

DELETE FROM dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta
WHERE ProductID = 710

-- COMMAND ----------

SELECT ProductID, Name, ListPrice, _change_type, _commit_version, _commit_timestamp
FROM table_changes('dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta', 2)
ORDER BY _commit_version, ProductID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![Screenshot 2026-10-07 132302.png](./Screenshot 2026-10-07 132302.png "Screenshot 2026-10-07 132302.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Target table: the state of the source before the changes (version 1)

-- COMMAND ----------

CREATE OR REPLACE TABLE dbr_dev_ua5816bd.roksolana_shendiu770_lab10.dim_product_scd2 AS
SELECT ProductID, Name, Color, ListPrice,
       TIMESTAMP'2026-10-07 10:18:05' AS valid_from,
       TRY_CAST(NULL AS TIMESTAMP) AS valid_to,
       true AS is_current
FROM dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Incremental MERGE (SCD Type 2)

-- COMMAND ----------

MERGE INTO dbr_dev_ua5816bd.roksolana_shendiu770_lab10.dim_product_scd2 AS t
USING (
  WITH changes AS (
    SELECT ProductID, Name, Color, ListPrice, _change_type, _commit_version, _commit_timestamp
    FROM table_changes('dbr_dev_ua5816bd.roksolana_shendiu770_lab10.product_delta', 2)
    WHERE _change_type <> 'update_preimage'
  ),
  latest AS (
    SELECT * FROM changes
    QUALIFY row_number() OVER (PARTITION BY ProductID ORDER BY _commit_version DESC) = 1
  )
  SELECT ProductID AS merge_key, * FROM latest
  UNION ALL
  SELECT TRY_CAST(NULL AS INT) AS merge_key, * FROM latest
  WHERE _change_type = 'update_postimage'
) AS s
ON t.ProductID = s.merge_key AND t.is_current = true
WHEN MATCHED AND s._change_type IN ('update_postimage', 'delete') THEN
  UPDATE SET t.is_current = false, t.valid_to = s._commit_timestamp
WHEN NOT MATCHED AND (s._change_type = 'insert' OR s.merge_key IS NULL) THEN
  INSERT (ProductID, Name, Color, ListPrice, valid_from, valid_to, is_current)
  VALUES (s.ProductID, s.Name, s.Color, s.ListPrice, s._commit_timestamp, NULL, true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Check the history

-- COMMAND ----------

SELECT ProductID, Name, ListPrice, valid_from, valid_to, is_current
FROM dbr_dev_ua5816bd.roksolana_shendiu770_lab10.dim_product_scd2
WHERE ProductID IN (680, 710, 1001)
ORDER BY ProductID, valid_from