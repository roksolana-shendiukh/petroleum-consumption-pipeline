from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col


def _build_kafka_options():
    eh_namespace = spark.conf.get("wikipedia.eh_namespace")
    eh_name = spark.conf.get("wikipedia.eh_name")
    secret_scope = spark.conf.get("wikipedia.secret_scope")
    secret_key = spark.conf.get("wikipedia.secret_key")

    starting_offsets = spark.conf.get("wikipedia.starting_offsets", "latest")
    max_offsets_per_trigger = spark.conf.get("wikipedia.max_offsets_per_trigger", "10000")

    eh_conn_str = dbutils.secrets.get(scope=secret_scope, key=secret_key)

    return {
        "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:9093",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": (
            'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{eh_conn_str}";'
        ),
        "subscribe": eh_name,
        "startingOffsets": starting_offsets,
        "maxOffsetsPerTrigger": max_offsets_per_trigger,
        "failOnDataLoss": "false",
    }


@dp.table(
    name="wikipedia_recentchange_ldp_bronze",
    comment="Raw Wikipedia recentchange events from Event Hub — unparsed JSON text, no schema applied",
    table_properties={
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true"
    }
)
def wikipedia_recentchange_ldp_bronze():
    options = _build_kafka_options()
    return (
        spark.readStream
            .format("kafka")
            .options(**options)
            .load()
            .withColumn("value", col("value").cast("string"))
            .withColumn("key", col("key").cast("string"))
            .withColumn("_ingested_at", current_timestamp())
    )