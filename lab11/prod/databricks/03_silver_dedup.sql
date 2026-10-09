USE CATALOG dbr_dev_ua_5816_trail;
USE SCHEMA zerobus_ais_roksolana;

CREATE TABLE IF NOT EXISTS silver_positions
CLUSTER BY (mmsi, event_ts)
COMMENT 'AIS positions, one row per event_id'
AS SELECT * EXCEPT (raw_payload) FROM bronze_positions WHERE false;

CREATE TABLE IF NOT EXISTS silver_ship_static
CLUSTER BY (mmsi, event_ts)
COMMENT 'AIS static ship data, one row per event_id'
AS SELECT * EXCEPT (raw_payload) FROM bronze_ship_static WHERE false;

MERGE INTO silver_positions AS target
USING (
  SELECT * EXCEPT (raw_payload, copy_rank)
  FROM (
    SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts, producer_id) AS copy_rank
    FROM bronze_positions
  )
  WHERE copy_rank = 1
) AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO silver_ship_static AS target
USING (
  SELECT * EXCEPT (raw_payload, copy_rank)
  FROM (
    SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts, producer_id) AS copy_rank
    FROM bronze_ship_static
  )
  WHERE copy_rank = 1
) AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT *;