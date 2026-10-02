from petroleum_transformations.quality import failed_rules

RULES = {"id_present": "id IS NOT NULL", "name_present": "name IS NOT NULL"}


def test_failed_rules_lists_every_broken_rule(spark):
    df = spark.createDataFrame([(1, 1, "a"), (2, None, None), (3, None, "b")], "k int, id int, name string")

    rows = {r["k"]: r["failed"] for r in df.withColumn("failed", failed_rules(RULES)).collect()}

    assert rows[1] == []
    assert sorted(rows[2]) == ["id_present", "name_present"]
    assert rows[3] == ["id_present"]


def test_failed_rules_treats_a_null_result_as_a_failure(spark):
    df = spark.createDataFrame([(None,)], "x int")

    result = df.select(failed_rules({"positive": "x > 0"}).alias("failed")).collect()[0]["failed"]

    assert result == ["positive"]