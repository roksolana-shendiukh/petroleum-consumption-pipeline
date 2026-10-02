from petroleum_transformations.reference import PRODUCT_PRICE_MAPPING


def test_active_products_have_a_known_price_route_and_code():
    for row in PRODUCT_PRICE_MAPPING:
        if row["is_active"]:
            assert row["price_route"] in {"gnd", "spt"}
            assert row["price_product_code"]


def test_consumption_codes_are_unique():
    codes = [row["consumption_product_code"] for row in PRODUCT_PRICE_MAPPING]

    assert len(codes) == len(set(codes))