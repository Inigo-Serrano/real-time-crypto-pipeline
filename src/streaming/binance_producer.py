import json
import os
from datetime import datetime, timezone

from binance import Client, ThreadedWebsocketManager
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
 
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
USERNAME = os.getenv("KAFKA_USERNAME")
PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC = os.getenv("KAFKA_QUOTES_TOPIC", "crypto-quotes")

# Configuramos Binance
SYMBOL = "LINKUSDT"
INTERVAL = Client.KLINE_INTERVAL_1MINUTE # Establecemos el intervalo de las velas a 1 minuto 
 
producer = None

def validate_config():
    required = {
        "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
        "KAFKA_USERNAME": USERNAME,
        "KAFKA_PASSWORD": PASSWORD,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"Missing environment variables: {', '.join(missing)}"
        )
    
# Convierte milisegundos a timestamp legible en UTC
def ms_to_utc_string(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
 
# Convierte milisegundos a timestamp legible en UTC
def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT", # Protocolo de seguridad para autenticación
        sasl_mechanism="PLAIN", # Mecanismo de autenticación SASL
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"), # Serializador para valor del mensaje
        key_serializer=lambda v: v.encode("utf-8") # Serializador para clave del mensaje
    )
 
# Esta función que se ejecuta cada vez que llega un mensaje de Binance
def handle_kline(msg):
    global producer
 
    try:
        print("Mensaje recibido de Binance:")
        print(msg)

        # Ignorar errores
        if msg.get("e") == "error":
            print(msg)
            return
        
        k = msg.get("k")
        if not k:
            return
        
        # Solo enviamos cuando la vela esté cerrada
        if not k.get("x", False):
            print("La vela todavía está abierta. No se envía a Kafka.\n")
            return
        
        # Nos quedamos solo con los campos que nos interesan
        symbol = k["s"]
        close_time_ms = k["T"]
        close_price = float(k["c"])
        volume = float(k["v"])

        # Con esto construimos el mensaje que vamos a enviar
        value = {
            "symbol": symbol,
            "@timestamp": ms_to_utc_string(close_time_ms),
            "close": close_price,
            "volume": volume
        }
 
        key = symbol
 
        print("Intentando enviar a Kafka...") # Printeamos info del mensaje
        print("Topic:", TOPIC)
        print("Key:", key)
        print("Value:", value)

        # Enviamos a Kafka
        future = producer.send(topic=TOPIC, key=key, value=value)
        record_metadata = future.get(timeout=10) # Espera confirmación
        producer.flush()
 
        print("Mensaje enviado correctamente")
        print("Topic:", record_metadata.topic)
        print("Partition:", record_metadata.partition)
        print("Offset:", record_metadata.offset)
        print()
 
    except Exception as e: # Printeamos el error si salta excepción
        print("ERROR DENTRO DE handle_kline")
        print(type(e).__name__)
        print(str(e))
 
 
 
def main():
    global producer

    validate_config()
 
    try:
        print("Creando producer Kafka...")
        producer = create_kafka_producer() # Creamos el producer Kafka
        print("Producer Kafka creado correctamente.")
 
        twm = ThreadedWebsocketManager() # Iniciamos WebSocket de Binance
        twm.start()
 
        twm.start_kline_socket( # Con esto escuchamos las velas a tiempo real cada minuto
            symbol=SYMBOL,
            interval=INTERVAL,
            callback=handle_kline
        )
 
        print(f"Escuchando {SYMBOL} en velas de 1 minuto...")
        print(f"Enviando a topic: {TOPIC}")
        print("Pulsa ENTER para salir.\n")
 
        input() # Para mantener programa corriendo hasta que se pulse ENTER
 
        twm.stop()
 
    except Exception as e:
        print("ERROR EN main()")
        print(type(e).__name__)
        print(str(e))
 
    finally:
        if producer is not None:
            producer.close()
 
 
if __name__ == "__main__":
    main()