from datetime import date

import pytest

from petroleum_transformations.bronze import (
    build_product_price_mapping,
    extract_product_codes,
    find_unmapped_codes,
    get_date_window,
    group_codes_by_route,
    prepare_bronze,
)

ROUTE_URLS = {"gnd": "http://gnd", "spt": "http://spt"}

MAPPING_ROWS = [
    {"consumption_product_code": "EPD0", "price_route": "gnd", "price_product_code": "P1"},
    {"consumption_product_code": "EPD0", "price_route": "spt", "price_product_code": "P2"},
    {"consumption_product_code": "EPJK", "price_route": "spt", "price_product_code": "P3"},
]


def test_get_date_window():
    assert get_date_window(30, today=date(2026, 9, 29)) == ("2026-08-30", "2026-09-29")


def test_build_product_price_mapping_groups_multiple_routes():
    m = build_product_price_mapping(MAPPING_ROWS)

    assert len(m["EPD0"]) == 2
    assert {"route": "gnd", "price_code": "P1"} in m["EPD0"]
    assert m["EPJK"] == [{"route": "spt", "price_code": "P3"}]


def test_extract_product_codes_skips_empty():
    data = [{"product": "EPD0"}, {"product": None}, {"x": 1}, {"product": "EPD0"}]

    assert extract_product_codes(data) == {"EPD0"}


def test_find_unmapped_codes():
    m = build_product_price_mapping(MAPPING_ROWS)

    assert find_unmapped_codes({"EPD0", "UNKNOWN"}, m) == {"UNKNOWN"}


def test_group_codes_by_route():
    m = build_product_price_mapping(MAPPING_ROWS)

    result = group_codes_by_route({"EPD0", "EPJK", "UNKNOWN"}, m, ROUTE_URLS)

    assert result == {"http://gnd": {"P1"}, "http://spt": {"P2", "P3"}}


def test_group_codes_by_route_unknown_route_raises():
    m = build_product_price_mapping(MAPPING_ROWS)

    with pytest.raises(KeyError):
        group_codes_by_route({"EPD0"}, m, {"gnd": "http://gnd"})  # 'spt' missing


def test_prepare_bronze_adds_metadata_and_dedupes(spark):
    df = spark.createDataFrame(
        [("2026-01-02", "S1", "1"), ("2026-01-02", "S1", "1"), ("2026-01-09", "S1", "2")],
        schema=["period", "series", "value"],
    )

    result = prepare_bronze(df)

    assert result.count() == 2
    assert {"ingestion_timestamp", "load_date"} <= set(result.columns)
    assert result.filter("ingestion_timestamp is null or load_date is null").count() == 0