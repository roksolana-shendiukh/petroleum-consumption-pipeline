from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

EH_NAMESPACE = spark.conf.get("wikipedia.eh_namespace")
EH_NAME = spark.conf.get("wikipedia.eh_name")
SECRET_SCOPE = spark.conf.get("wikipedia.secret_scope")
SECRET_KEY = spark.conf.get("wikipedia.secret_key")
STARTING_OFFSETS = spark.conf.get("wikipedia.starting_offsets")
MAX_OFFSETS_PER_TRIGGER = spark.conf.get("wikipedia.max_offsets_per_trigger")

EH_CONN_STR = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

EH_KAFKA_ENDPOINT = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_SASL = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" password="{EH_CONN_STR}";'
)


@dp.table(
    name="wikipedia_recentchange_ldp_bronze",
    comment="Raw Wikipedia recentchange events from Event Hub — untouched Kafka payload, no parsing"
)
def wikipedia_recentchange_ldp_bronze():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", EH_KAFKA_ENDPOINT)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", EH_SASL)
            .option("subscribe", EH_NAME)
            .option("startingOffsets", STARTING_OFFSETS)
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
            .option("failOnDataLoss", "false")
            .load()
            .withColumn("_ingested_at", current_timestamp())
    )