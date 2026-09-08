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

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

TOPIC_IN = os.getenv(
    "KAFKA_VWAP_TOPIC",
    "crypto-vwap"
)

GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "crypto-pipeline"
)

REGION = os.getenv(
    "AWS_REGION",
    "eu-west-1"
)

AWS_PROFILE = os.getenv("AWS_PROFILE")

DATABASE = os.getenv("TIMESTREAM_DATABASE")
TABLE = os.getenv("TIMESTREAM_VWAP_TABLE")

def validate_config():
    required = {
        "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
        "KAFKA_USERNAME": KAFKA_USERNAME,
        "KAFKA_PASSWORD": KAFKA_PASSWORD,
        "TIMESTREAM_DATABASE": DATABASE,
        "TIMESTREAM_VWAP_TABLE": TABLE,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"Missing environment variables: {', '.join(missing)}"
        )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def iso_to_epoch_ms(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def build_record(msg: dict) -> dict: # Aquí la cosa cambia: recibimos el mensaje del topic de VWAP
    # El mensaje contendrá: symbol, window_start, window_end y vwap
    symbol       = msg["symbol"]
    window_start = msg["window_start"]
    window_end   = msg["window_end"]
    vwap         = float(msg["vwap"])

    epoch_ms = iso_to_epoch_ms(window_end)

    return {
        "Dimensions": [
            {"Name": "symbol",       "Value": symbol}, # La dimensión sigue siendo el símbolo, pero ahora añadimos la ventana de tiempo
            {"Name": "source_topic", "Value": TOPIC_IN},
            {"Name": "window_start", "Value": window_start},
            {"Name": "window_end",   "Value": window_end},
        ],
        "MeasureName":      "vwap", # Aquí guardamos el VWAP
        "MeasureValue":     str(vwap),
        "MeasureValueType": "DOUBLE",
        "Time":             epoch_ms,
        "TimeUnit":         "MILLISECONDS",
        "Version":          int(time.time() * 1000),
    }


def write_to_timestream(client, record: dict) -> None:
    try:
        client.write_records(
            DatabaseName=DATABASE,
            TableName=TABLE,
            Records=[record],
        )
        log.info(
            "✅ Timestream OK | symbol=%s | vwap=%s | window_end=%s",
            record["Dimensions"][0]["Value"],
            record["MeasureValue"],
            record["Dimensions"][3]["Value"],
        )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "RejectedRecordsException":
            rejected = exc.response.get("RejectedRecords", [])
            log.warning("Registros rechazados: %s", rejected)
        else:
            log.error("Error Timestream: %s", exc)
            raise

def main() -> None:
    validate_config()
    log.info("Iniciando consumer HU-8 (VWAP) …")
    log.info("Topic Kafka : %s", TOPIC_IN)
    log.info("Timestream  : %s / %s @ %s", DATABASE, TABLE, REGION)

    session = boto3.Session(
    profile_name=AWS_PROFILE or None,
    region_name=REGION,
)

    ts_client = session.client("timestream-write")

    # Consumer Kafka
    consumer = KafkaConsumer(
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
        for kafka_record in consumer:
            try:
                msg = kafka_record.value
                log.debug("Mensaje recibido: %s", msg)

                record = build_record(msg)
                write_to_timestream(ts_client, record)

            except (KeyError, ValueError) as exc:
                log.warning("Mensaje malformado, se descarta: %s | Error: %s", kafka_record.value, exc)
            except Exception as exc:
                log.error("Error inesperado procesando mensaje: %s", exc)

    except KeyboardInterrupt:
        log.info("Interrumpido por el usuario.")
    finally:
        consumer.close()
        log.info("Consumer cerrado.")


if __name__ == "__main__":
    main()
