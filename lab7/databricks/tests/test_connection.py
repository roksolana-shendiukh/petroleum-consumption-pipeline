def test_spark_works(spark):
    assert spark.range(3).count() == 3