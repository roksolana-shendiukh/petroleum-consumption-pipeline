USE CATALOG dbr_dev_ua_5816_trail;

ALTER TABLE zerobus_ais_roksolana.bronze_positions
ADD COLUMN raw_payload VARIANT COMMENT 'Original aisstream message, kept for reprocessing';

ALTER TABLE zerobus_ais_roksolana.bronze_ship_static
ADD COLUMN raw_payload VARIANT COMMENT 'Original aisstream message, kept for reprocessing';