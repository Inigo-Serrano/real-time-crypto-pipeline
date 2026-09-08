

import json
import logging
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()
# Configuraciones Kafka y Timestream
AWS_PROFILE = os.getenv("AWS_PROFILE")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

TOPIC_IN = os.getenv(
    "KAFKA_QUOTES_TOPIC",
    "crypto-quotes"
)

GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "crypto-pipeline"
)

REGION = os.getenv(
    "AWS_REGION",
    "eu-west-1"
)

DATABASE = os.getenv("TIMESTREAM_DATABASE")
TABLE = os.getenv("TIMESTREAM_QUOTES_TABLE")

def validate_config():
    required = {
        "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
        "KAFKA_USERNAME": KAFKA_USERNAME,
        "KAFKA_PASSWORD": KAFKA_PASSWORD,
        "TIMESTREAM_DATABASE": DATABASE,
        "TIMESTREAM_QUOTES_TABLE": TABLE,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"Missing environment variables: {', '.join(missing)}"
        )

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def iso_to_epoch_ms(iso_str: str) -> str: # Con esta función se convierte timestamp ISO a epochs en milisegundos
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def build_record(msg: dict) -> dict: # Esta función construye el registro Timestream a partir del mensaje Kafka
    # El mensaje contendrá: timestamp, symbol, close y volume
    symbol      = msg["symbol"]
    timestamp   = msg["@timestamp"]
    close       = float(msg["close"])
    volume      = float(msg["volume"])

    epoch_ms = iso_to_epoch_ms(timestamp) # Convertimos a epochs

    return { # Construimos el registro en formato Timestream con las dimensiones y medidas correspondientes
        "Dimensions": [
            {"Name": "symbol",       "Value": symbol},
            {"Name": "source_topic", "Value": TOPIC_IN},
            {"Name": "event_ts",     "Value": timestamp},
        ],
        "MeasureName":      "close", # Almacenaremos el precio de cierre
        "MeasureValue":     str(close),
        "MeasureValueType": "DOUBLE",
        "Time":             epoch_ms,
        "TimeUnit":         "MILLISECONDS",
        "Version":          int(time.time() * 1000),
    }


def write_to_timestream(client, record: dict) -> None: # Con esta función escribimos el registro en Timestream y manejamos posibles errores
    try:
        client.write_records( # Escribimos el registro en Timestream
            DatabaseName=DATABASE,
            TableName=TABLE,
            Records=[record],
        )
        log.info( # Logueamos el resultado si funciona
            "✅ Timestream OK | symbol=%s | close=%s | ts=%s",
            record["Dimensions"][0]["Value"],
            record["MeasureValue"],
            record["Dimensions"][2]["Value"],
        )
    except ClientError as exc: # Aquí manejas los posibles errores que puedan darse (como de rechazo de registros o de conexión)
        code = exc.response["Error"]["Code"]
        if code == "RejectedRecordsException":
            rejected = exc.response.get("RejectedRecords", [])
            log.warning("Registros rechazados: %s", rejected)
        else:
            log.error("Error Timestream: %s", exc)
            raise

def main() -> None: # Función principal
    validate_config()
    log.info("Iniciando consumer HU-8 (quotes) …")
    log.info("Topic Kafka : %s", TOPIC_IN)
    log.info("Timestream  : %s / %s @ %s", DATABASE, TABLE, REGION)

    # Cliente Timestream
    session = boto3.Session(
    profile_name=AWS_PROFILE or None,
    region_name=REGION,
)

    ts_client = session.client("timestream-write")

    # Consumer Kafka
    consumer = KafkaConsumer( # Configuramos el consumer de Kafka con las credenciales y parámetros necesarios para consumir los mensajes
        TOPIC_IN,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    log.info("Consumer conectado. Esperando mensajes…  (Ctrl+C para salir)")

    try:
        for kafka_record in consumer: # Iteramos sobre los mensajes que llegan del topic Kafka
            try:
                msg = kafka_record.value # Obtenemos el valor del mensaje
                log.debug("Mensaje recibido: %s", msg)

                record = build_record(msg) # Construimos el registro Timestream a partir del mensaje Kafka
                write_to_timestream(ts_client, record) # Escribimos el registro en Timestream

            except (KeyError, ValueError) as exc: # Manejo de errores
                log.warning("Mensaje malformado, se descarta: %s | Error: %s", kafka_record.value, exc)
            except Exception as exc:
                log.error("Error inesperado procesando mensaje: %s", exc)

    except KeyboardInterrupt: # Permite salir del bucle con Ctrl + C
        log.info("Interrumpido por el usuario.")
    finally:
        consumer.close()
        log.info("Consumer cerrado.")


if __name__ == "__main__":
    main()
