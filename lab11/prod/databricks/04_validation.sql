USE CATALOG dbr_dev_ua_5816_trail;
USE SCHEMA zerobus_ais_roksolana;

SELECT 'runs' AS check_name, 'positions' AS tbl, producer_id, count(*) AS rows,
       min(ingest_ts) AS first_ingest, max(ingest_ts) AS last_ingest,
       round(count(*) / greatest(timestampdiff(SECOND, min(ingest_ts), max(ingest_ts)), 1), 1) AS rows_per_second
FROM bronze_positions
GROUP BY producer_id
UNION ALL
SELECT 'runs', 'static', producer_id, count(*), min(ingest_ts), max(ingest_ts),
       round(count(*) / greatest(timestampdiff(SECOND, min(ingest_ts), max(ingest_ts)), 1), 1)
FROM bronze_ship_static
GROUP BY producer_id
ORDER BY first_ingest, tbl;

SELECT 'sequence' AS check_name, 'positions' AS tbl, producer_id, count(*) AS rows,
       count(DISTINCT producer_seq) AS distinct_seq, max(producer_seq) AS max_seq,
       count(*) - count(DISTINCT producer_seq) AS replayed_rows,
       max(producer_seq) - count(DISTINCT producer_seq) AS missing_seq
FROM bronze_positions
GROUP BY producer_id
UNION ALL
SELECT 'sequence', 'static', producer_id, count(*), count(DISTINCT producer_seq), max(producer_seq),
       count(*) - count(DISTINCT producer_seq), max(producer_seq) - count(DISTINCT producer_seq)
FROM bronze_ship_static
GROUP BY producer_id
ORDER BY producer_id, tbl;

SELECT 'duplicates' AS check_name, 'positions' AS tbl,
       (SELECT count(*) FROM bronze_positions) AS bronze_rows,
       (SELECT count(DISTINCT event_id) FROM bronze_positions) AS bronze_events,
       (SELECT count(*) FROM bronze_positions) - (SELECT count(DISTINCT event_id) FROM bronze_positions) AS duplicate_rows,
       (SELECT count(*) FROM silver_positions) AS silver_rows,
       (SELECT count(DISTINCT event_id) FROM silver_positions) AS silver_events
UNION ALL
SELECT 'duplicates', 'static',
       (SELECT count(*) FROM bronze_ship_static),
       (SELECT count(DISTINCT event_id) FROM bronze_ship_static),
       (SELECT count(*) FROM bronze_ship_static) - (SELECT count(DISTINCT event_id) FROM bronze_ship_static),
       (SELECT count(*) FROM silver_ship_static),
       (SELECT count(DISTINCT event_id) FROM silver_ship_static);

WITH copies AS (
  SELECT 'positions' AS tbl, event_id, regexp_extract(producer_id, '^(.+)-\\d{8}T\\d{6}Z$', 1) AS producer
  FROM bronze_positions
  UNION ALL
  SELECT 'static', event_id, regexp_extract(producer_id, '^(.+)-\\d{8}T\\d{6}Z$', 1)
  FROM bronze_ship_static
),
events AS (
  SELECT tbl, event_id, count(*) AS copies, array_join(sort_array(collect_set(producer)), '+') AS seen_by
  FROM copies
  GROUP BY tbl, event_id
)
SELECT 'overlap' AS check_name, tbl, seen_by, count(*) AS events, sum(copies) AS bronze_rows
FROM events
GROUP BY tbl, seen_by
ORDER BY tbl, seen_by;

SELECT 'source_lag' AS check_name, 'positions' AS tbl,
       round(avg(timestampdiff(MILLISECOND, event_ts, ingest_ts))) AS avg_ms,
       percentile_approx(timestampdiff(MILLISECOND, event_ts, ingest_ts), 0.5) AS p50_ms,
       percentile_approx(timestampdiff(MILLISECOND, event_ts, ingest_ts), 0.95) AS p95_ms,
       max(timestampdiff(MILLISECOND, event_ts, ingest_ts)) AS max_ms
FROM silver_positions
UNION ALL
SELECT 'source_lag', 'static',
       round(avg(timestampdiff(MILLISECOND, event_ts, ingest_ts))),
       percentile_approx(timestampdiff(MILLISECOND, event_ts, ingest_ts), 0.5),
       percentile_approx(timestampdiff(MILLISECOND, event_ts, ingest_ts), 0.95),
       max(timestampdiff(MILLISECOND, event_ts, ingest_ts))
FROM silver_ship_static;

SELECT 'freshness' AS check_name, 'positions' AS tbl,
       max(ingest_ts) AS last_ingest, current_timestamp() AS queried_at,
       timestampdiff(MILLISECOND, max(ingest_ts), current_timestamp()) AS ms_since_last_ingest
FROM bronze_positions
UNION ALL
SELECT 'freshness', 'static', max(ingest_ts), current_timestamp(),
       timestampdiff(MILLISECOND, max(ingest_ts), current_timestamp())
FROM bronze_ship_static;

SELECT 'normalization' AS check_name, message_type, count(*) AS rows,
       count_if(raw_payload:Message.PositionReport.TrueHeading::int = 511) AS raw_heading_511,
       count_if(true_heading IS NULL) AS heading_null,
       count_if(raw_payload:Message.PositionReport.Cog::double >= 360) AS raw_cog_360,
       count_if(cog IS NULL) AS cog_null,
       count_if(raw_payload:Message.PositionReport.RateOfTurn::int = -128) AS raw_rot_minus_128,
       count_if(rate_of_turn IS NULL) AS rot_null
FROM bronze_positions
WHERE message_type = 'PositionReport'
GROUP BY message_type;

SELECT 'cost' AS check_name, u.usage_date, u.sku_name,
       u.product_features.lakeflow_connect.zerobus_request_type AS zerobus_request_type,
       sum(u.usage_quantity) AS usage_quantity, u.usage_unit,
       round(sum(u.usage_quantity * p.pricing.default), 4) AS list_cost_usd
FROM system.billing.usage AS u
LEFT JOIN system.billing.list_prices AS p
  ON u.sku_name = p.sku_name
 AND u.cloud = p.cloud
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.billing_origin_product = 'LAKEFLOW_CONNECT'
  AND u.usage_date >= DATE'2026-10-08'
GROUP BY ALL
ORDER BY u.usage_date;