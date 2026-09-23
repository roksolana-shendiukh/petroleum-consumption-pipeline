from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col


def _get_eh_connection_config():
    return {
        "eh_namespace": spark.conf.get("wikipedia.eh_namespace"),
        "eh_name": spark.conf.get("wikipedia.eh_name"),
        "secret_scope": spark.conf.get("wikipedia.secret_scope"),
        "secret_key": spark.conf.get("wikipedia.secret_key"),
    }


def _get_eh_stream_config():
    return {
        "starting_offsets": spark.conf.get("wikipedia.starting_offsets", "latest"),
        "max_offsets_per_trigger": spark.conf.get("wikipedia.max_offsets_per_trigger", "10000"),
    }


def _build_eh_sasl_config(secret_scope, secret_key):
    eh_conn_str = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    return (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{eh_conn_str}";'
    )

def _build_kafka_options():
    conn = _get_eh_connection_config()
    stream = _get_eh_stream_config()

    return {
        "kafka.bootstrap.servers": f"{conn['eh_namespace']}.servicebus.windows.net:9093",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": _build_eh_sasl_config(conn["secret_scope"], conn["secret_key"]),
        "subscribe": conn["eh_name"],
        "startingOffsets": stream["starting_offsets"],
        "maxOffsetsPerTrigger": stream["max_offsets_per_trigger"],
        "failOnDataLoss": "false",
    }


@dp.table(
    name="wikipedia_recentchange_ldp_bronze",
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