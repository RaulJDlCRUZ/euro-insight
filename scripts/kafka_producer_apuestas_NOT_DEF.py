"""
producer_apuestas.py
--------------------
Generador sintético de eventos de compra de boletos de Euromillones.
Publica en el topic Kafka `apuestas-raw` con un intervalo aleatorio
de 100-500 ms entre mensajes.

Dependencias:
    pip install kafka-python

Uso:
    python producer_apuestas.py [--bootstrap localhost:9094] [--topic apuestas-raw] [--limit 0]

    --limit  Número máximo de eventos a emitir (0 = infinito).
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("producer_apuestas")

# ---------------------------------------------------------------------------
# Datos de referencia
# ---------------------------------------------------------------------------

# Códigos de provincia INE (muestra representativa)
PROVINCIAS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
    "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
    "51", "52",
]

TIPOS_APUESTA = ["simple", "multiple", "aleatorio"]
CANALES      = ["presencial", "online", "app"]

# Importes habituales en Euromillones (€)
IMPORTES = [2.50, 5.00, 7.50, 10.00, 15.00, 20.00, 25.00, 50.00]

# ---------------------------------------------------------------------------
# Generación de eventos
# ---------------------------------------------------------------------------

def _timestamp_iso() -> str:
    """Devuelve el instante actual en formato ISO 8601 con milisegundos y sufijo Z."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _id_transaccion(ts_iso: str) -> str:
    """
    Formato: TXN-YYYYMMDD-HHMMSS-<sufijo aleatorio 3 dígitos>
    El sufijo evita colisiones cuando el intervalo es muy corto.
    """
    compact = ts_iso[:19].replace("-", "").replace("T", "-").replace(":", "")
    suffix  = random.randint(1, 999)
    return f"TXN-{compact}-{suffix:03d}"


def _id_administracion(codigo_provincia: str) -> str:
    """Genera un ID de administración coherente con la provincia."""
    numero = random.randint(1, 99)
    return f"ADM-{codigo_provincia}-{numero:03d}"


def generar_evento() -> dict:
    """Construye un evento sintético de apuesta."""
    ts              = _timestamp_iso()
    codigo_prov     = random.choice(PROVINCIAS)

    return {
        "timestamp":         ts,
        "id_transaccion":    _id_transaccion(ts),
        "codigo_provincia":  codigo_prov,
        "id_administracion": _id_administracion(codigo_prov),
        "tipo_apuesta":      random.choice(TIPOS_APUESTA),
        "numeros":           sorted(random.sample(range(1, 51), 5)),
        "estrellas":         sorted(random.sample(range(1, 13), 2)),
        "importe":           random.choice(IMPORTES),
        "canal":             random.choice(CANALES),
    }

# ---------------------------------------------------------------------------
# Callbacks Kafka
# ---------------------------------------------------------------------------

def _on_send_success(record_metadata):
    log.debug(
        "✓ topic=%s  partition=%d  offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def _on_send_error(exc):
    log.error("✗ Error al publicar mensaje: %s", exc)

# ---------------------------------------------------------------------------
# Productor principal
# ---------------------------------------------------------------------------

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        # Garantías de entrega: esperar confirmación del líder
        acks="all",
        retries=3,
        retry_backoff_ms=300,
        # Compresión ligera para JSON
        compression_type="gzip",
        # Identificador visible en las métricas de Kafka
        client_id="producer-apuestas-sim",
    )


def run(bootstrap_servers: str, topic: str, limit: int) -> None:
    log.info("Conectando a Kafka en %s …", bootstrap_servers)
    producer = build_producer(bootstrap_servers)
    log.info("Productor listo. Topic destino: `%s`", topic)

    # Manejo limpio de SIGINT / SIGTERM
    stop = {"flag": False}

    def _handler(sig, frame):
        log.info("Señal %s recibida — cerrando productor …", sig)
        stop["flag"] = True

    signal.signal(signal.SIGINT,  _handler)
    signal.signal(signal.SIGTERM, _handler)

    enviados = 0

    while not stop["flag"]:
        if limit and enviados >= limit:
            log.info("Límite de %d eventos alcanzado. Fin.", limit)
            break

        evento = generar_evento()

        try:
            (
                producer
                .send(topic, value=evento)
                .add_callback(_on_send_success)
                .add_errback(_on_send_error)
            )
            enviados += 1

            # Log cada 50 eventos para no saturar la consola
            if enviados % 50 == 0:
                log.info("Eventos publicados: %d", enviados)
            else:
                log.debug("Evento publicado: %s", evento["id_transaccion"])

        except KafkaError as exc:
            log.error("KafkaError al enviar evento: %s", exc)

        # Intervalo aleatorio 100-500 ms
        delay = random.uniform(0.1, 0.5)
        time.sleep(delay)

    # Flush y cierre ordenado
    log.info("Haciendo flush del buffer …")
    producer.flush(timeout=10)
    producer.close()
    log.info("Productor cerrado. Total eventos enviados: %d", enviados)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generador sintético de eventos de apuestas → Kafka"
    )
    parser.add_argument(
        "--bootstrap",
        default="localhost:9094",
        help="Bootstrap server de Kafka (default: localhost:9094)",
    )
    parser.add_argument(
        "--topic",
        default="apuestas-raw",
        help="Topic Kafka destino (default: apuestas-raw)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Número máximo de eventos a emitir (0 = infinito)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        limit=args.limit,
    )
