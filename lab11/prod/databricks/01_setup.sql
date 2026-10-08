USE CATALOG dbr_dev_ua_5816_trail;

CREATE SCHEMA IF NOT EXISTS zerobus_ais_roksolana
COMMENT 'Lab 11: AIS events ingested directly through Zerobus Ingest';

CREATE TABLE IF NOT EXISTS zerobus_ais_roksolana.bronze_positions (
  event_id STRING NOT NULL COMMENT 'SHA-256 of message_type, mmsi, time_utc, latitude, longitude',
  message_type STRING NOT NULL COMMENT 'PositionReport, StandardClassBPositionReport or ExtendedClassBPositionReport',
  mmsi BIGINT NOT NULL,
  event_ts TIMESTAMP NOT NULL COMMENT 'MetaData.time_utc, truncated to microseconds',
  ingest_ts TIMESTAMP NOT NULL COMMENT 'Time the producer sent the record',
  producer_id STRING NOT NULL,
  producer_seq BIGINT NOT NULL,
  ship_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  sog DOUBLE COMMENT 'Speed over ground, knots',
  cog DOUBLE COMMENT 'Course over ground, degrees',
  true_heading INT,
  nav_status INT COMMENT 'Class A only',
  rate_of_turn INT COMMENT 'Class A only',
  position_accuracy BOOLEAN,
  raim BOOLEAN,
  utc_second INT,
  message_id INT,
  repeat_indicator INT
)
CLUSTER BY (mmsi, event_ts)
COMMENT 'Raw AIS positions, normalized by the producer, append-only, may contain duplicates';

CREATE TABLE IF NOT EXISTS zerobus_ais_roksolana.bronze_ship_static (
  event_id STRING NOT NULL COMMENT 'SHA-256 of message_type, mmsi, time_utc, report_part',
  message_type STRING NOT NULL COMMENT 'ShipStaticData, StaticDataReport or ExtendedClassBPositionReport',
  mmsi BIGINT NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  ingest_ts TIMESTAMP NOT NULL,
  producer_id STRING NOT NULL,
  producer_seq BIGINT NOT NULL,
  report_part STRING COMMENT 'full, A or B',
  ship_name STRING,
  call_sign STRING,
  imo_number INT,
  ship_type INT,
  dim_to_bow INT,
  dim_to_stern INT,
  dim_to_port INT,
  dim_to_starboard INT,
  draught DOUBLE,
  destination STRING,
  eta_month INT,
  eta_day INT,
  eta_hour INT,
  eta_minute INT
)
CLUSTER BY (mmsi, event_ts)
COMMENT 'Raw AIS static ship data, normalized by the producer, append-only, may contain duplicates';

GRANT USE SCHEMA ON SCHEMA dbr_dev_ua_5816_trail.zerobus_ais_roksolana TO `<sp-application-id>`;
GRANT MODIFY, SELECT ON TABLE dbr_dev_ua_5816_trail.zerobus_ais_roksolana.bronze_positions TO `<sp-application-id>`;
GRANT MODIFY, SELECT ON TABLE dbr_dev_ua_5816_trail.zerobus_ais_roksolana.bronze_ship_static TO `<sp-application-id>`;