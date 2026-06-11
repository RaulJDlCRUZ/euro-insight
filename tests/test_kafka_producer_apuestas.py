"""
kafka_producer_apuestas.py
──────────────────────────
Producer de prueba que genera eventos sintéticos de apuestas
y los publica en el tópico `apuestas-raw`.

Dependencias:
    pip install kafka-python faker

Uso:
    python kafka_producer_apuestas.py              # genera indefinidamente
    python kafka_producer_apuestas.py --n 500      # genera 500 eventos y para
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC             = "apuestas-raw"

PROVINCIAS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
    "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
    "51", "52",
]
TIPOS_APUESTA = ["simple", "multiple", "aleatorio"]
CANALES       = ["presencial", "online", "app"]

# ──────────────────────────────────────────────
# Generación de eventos
# ──────────────────────────────────────────────

def generar_evento(introducir_retraso: bool = False) -> dict:
    """
    Construye un evento que sigue el contrato del tópico apuestas-raw.

    Si `introducir_retraso=True`, simula eventos tardíos (hasta 90 s de retraso)
    para ejercitar el watermark de 2 minutos del pipeline.
    """
    # Event-time: momento real del evento (puede ser pasado si hay retraso)
    retraso_segundos = random.uniform(0, 90) if introducir_retraso else 0
    ts = datetime.now(timezone.utc).timestamp() - retraso_segundos
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"

    provincia  = random.choice(PROVINCIAS)
    adm_seq    = random.randint(1, 50)

    return {
        "timestamp":        ts_iso,
        "id_transaccion":   f"TXN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid4())[:8].upper()}",
        "codigo_provincia": provincia,
        "id_administracion": f"ADM-{provincia}-{adm_seq:03d}",
        "tipo_apuesta":     random.choice(TIPOS_APUESTA),
        "numeros":          sorted(random.sample(range(1, 51), 5)),
        "estrellas":        sorted(random.sample(range(1, 13), 2)),
        "importe":          round(random.choice([1.0, 2.0, 2.5, 5.0, 10.0, 20.0]), 2),
        "canal":            random.choice(CANALES),
    }


# ──────────────────────────────────────────────
# Callbacks
# ──────────────────────────────────────────────

def on_send_success(record_metadata):
    print(
        f"    topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(excp: KafkaError):
    print(f"    Error al publicar: {excp}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main(n_eventos: int, intervalo_ms: int, retrasos: bool):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serialización a JSON (bytes UTF-8)
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        # Clave de partición: código de provincia → distribución uniforme
        key_serializer=lambda k: k.encode("utf-8"),
        # Fiabilidad: esperamos ACK del líder
        acks="all",
        # Reintentos automáticos ante fallos transitorios
        retries=3,
        retry_backoff_ms=200,
    )

    print(f"Producer conectado a {BOOTSTRAP_SERVERS}")
    print(f"Publicando en tópico '{TOPIC}' | retrasos={retrasos}")
    print("─" * 60)

    enviados = 0
    try:
        while n_eventos == 0 or enviados < n_eventos:
            evento = generar_evento(introducir_retraso=retrasos)
            producer.send(
                TOPIC,
                key=evento["codigo_provincia"],
                value=evento,
            ).add_callback(on_send_success).add_errback(on_send_error)

            enviados += 1
            if enviados % 50 == 0:
                producer.flush()
                print(f"  → {enviados} eventos publicados")

            time.sleep(intervalo_ms / 1000)

    except KeyboardInterrupt:
        print("\nProducer detenido por el usuario.")
    finally:
        producer.flush()
        producer.close()
        print(f"\nTotal publicados: {enviados}")


# ──────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Producer de apuestas sintéticas")
    parser.add_argument(
        "--n", type=int, default=0,
        help="Número de eventos a generar (0 = indefinido)",
    )
    parser.add_argument(
        "--intervalo-ms", type=int, default=200,
        help="Milisegundos entre eventos (default 200 ms → ~5 ev/s)",
    )
    parser.add_argument(
        "--retrasos", action="store_true",
        help="Introduce retrasos aleatorios (≤90 s) para ejercitar el watermark",
    )
    args = parser.parse_args()
    main(args.n, args.intervalo_ms, args.retrasos)
