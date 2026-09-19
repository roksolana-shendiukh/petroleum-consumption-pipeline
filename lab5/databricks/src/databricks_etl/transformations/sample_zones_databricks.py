from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

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

RAW_EVENT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("timestamp", StringType()),
    StructField("wiki", StringType()),
    StructField("server_name", StringType()),
    StructField("length", StringType()),
])


@dp.table(
    name="wikipedia_recentchange_bronze",
    comment="Raw Wikipedia recentchange events consumed from Event Hub via the Kafka protocol"
)
def wikipedia_recentchange_bronze():
    raw = (
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
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), RAW_EVENT_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    return (
        parsed.select("data.*", "kafka_timestamp")
            .withColumn("_ingested_at", current_timestamp())
    )