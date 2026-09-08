from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    from_json,
    sum as spark_sum,
    to_timestamp,
    struct,
    to_json,
    date_format,
    round as spark_round
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

import os

from dotenv import load_dotenv

load_dotenv()

# Iniciamos la Spark session
spark = (
    SparkSession.builder
    .appName("CryptoVWAPCalculator")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN") 

# Configuración de Kafka
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

TOPIC_IN = os.getenv(
    "KAFKA_QUOTES_TOPIC",
    "crypto-quotes"
)

TOPIC_OUT = os.getenv(
    "KAFKA_VWAP_TOPIC",
    "crypto-vwap"
)

CHECKPOINT_PATH = os.getenv(
    "SPARK_CHECKPOINT_PATH",
    "/tmp/checkpoint_vwap_link"
)

KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"

KAFKA_SASL_JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

# Esquema de los JSON de Binance
json_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", StringType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

required = {
    "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
    "KAFKA_USERNAME": KAFKA_USERNAME,
    "KAFKA_PASSWORD": KAFKA_PASSWORD,
}

missing = [name for name, value in required.items() if not value]

if missing:
    raise ValueError(
        f"Missing environment variables: {', '.join(missing)}"
    )

# Leemos el stream de Kafka con los datos
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .load()
)

# Parseamos el JSON a columnas
df_parsed = (
    df_raw
    .select(from_json(col("value").cast("string"), json_schema).alias("data"))
    .select("data.*")
)

# # Convertimos timestamp y limpiamos datos
df_with_ts = (
    df_parsed
    .withColumn(
        "event_time",
        to_timestamp(col("@timestamp"), "yyyy-MM-dd'T'HH:mm:ssX")
    )
    .filter( # Filtramos filas con datos faltantes o volumen cero
        col("symbol").isNotNull() &
        col("event_time").isNotNull() &
        col("close").isNotNull() &
        col("volume").isNotNull() &
        (col("volume") > 0)
    )
)

# Calculate VWAP over 5-minute event-time windows
df_vwap = (
    df_with_ts
    .withWatermark("event_time", "5 minutes")
    .withColumn("price_volume", col("close") * col("volume"))
    .groupBy(
        window(
    col("event_time"),
    "5 minutes"
),
        col("symbol")
    )
    .agg(
        spark_sum("price_volume").alias("sum_pv"),
        spark_sum("volume").alias("sum_vol")
    )
    .filter(col("sum_vol") > 0)
    .withColumn(
        "vwap",
        spark_round(col("sum_pv") / col("sum_vol"), 2)
    )
)

# Preparamos los datos para enviarlos a Kafka (convirtimos la ventana a formato ISO y serializamos a JSON)
df_to_kafka = (
    df_vwap
    .select(
        col("symbol").cast("string").alias("key"),
        to_json(
            struct(
                date_format(
                    col("window.start"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                ).alias("window_start"),
                date_format(
                    col("window.end"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                ).alias("window_end"),
                col("symbol"),
                col("vwap")
            )
        ).alias("value")
    )
)

# Finalmente, escribimos resultados en Kafka al topic de salida
query = (
    df_to_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", TOPIC_OUT)
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

print(f"Streaming iniciado. Enviando resultados a {TOPIC_OUT}...") # Mensaje de confirmación

query.awaitTermination() # Para mantener la aplicación corriendo
 