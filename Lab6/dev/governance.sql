-- Databricks notebook source
GRANT USE SCHEMA ON SCHEMA dbr_dev_ua5816bd.roksolana_shendiu770_gold TO `natalkamartinuk55@softserve.academy`;
GRANT SELECT ON SCHEMA dbr_dev_ua5816bd.roksolana_shendiu770_gold TO `natalkamartinuk55@softserve.academy`;

-- COMMAND ----------

ALTER TABLE dbr_dev_ua5816bd.roksolana_shendiu770_gold.dim_date
ALTER COLUMN `year` SET TAGS ('gold_access' = 'year_scope');

ALTER TABLE dbr_dev_ua5816bd.roksolana_shendiu770_gold.fct_snapshot_consumption_weekly
ALTER COLUMN series_bk SET TAGS ('gold_access' = 'technical_id');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dbr_dev_ua5816bd.roksolana_shendiu770_gold.year_from_2020_only(yr INT)
RETURNS BOOLEAN
RETURN yr >= 2020;

-- COMMAND ----------

CREATE POLICY restrict_dim_date_to_recent_years
ON TABLE dbr_dev_ua5816bd.roksolana_shendiu770_gold.dim_date
ROW FILTER dbr_dev_ua5816bd.roksolana_shendiu770_gold.year_from_2020_only
TO `natalkamartinuk55@softserve.academy`
FOR TABLES
MATCH COLUMNS has_tag_value('gold_access', 'year_scope') AS yr_col
USING COLUMNS (yr_col);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dbr_dev_ua5816bd.roksolana_shendiu770_gold.mask_technical_id(val STRING)
RETURNS STRING
RETURN '***';

-- COMMAND ----------

CREATE POLICY mask_series_bk
ON TABLE dbr_dev_ua5816bd.roksolana_shendiu770_gold.fct_snapshot_consumption_weekly
COLUMN MASK dbr_dev_ua5816bd.roksolana_shendiu770_gold.mask_technical_id
TO `natalkamartinuk55@softserve.academy`
FOR TABLES
MATCH COLUMNS has_tag_value('gold_access', 'technical_id') AS id_col
ON COLUMN id_col;

-- COMMAND ----------

SELECT MIN(`year`) FROM dbr_dev_ua5816bd.roksolana_shendiu770_gold.dim_date;


-- COMMAND ----------

SELECT series_bk FROM dbr_dev_ua5816bd.roksolana_shendiu770_gold.fct_snapshot_consumption_weekly LIMIT 5;