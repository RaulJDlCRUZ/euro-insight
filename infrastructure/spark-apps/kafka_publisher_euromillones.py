"""
kafka_publisher_euromillones.py
--------------------------------
Publisher semanal de resultados de Euromillones vía Kafka.

Flujo:
  1. Calcula la ventana lunes–domingo de la semana anterior.
  2. Hace scraping de loteriasyapuestas.es con Playwright.
  3. Limpia el payload (elimina contenidosRelacionados, nulls de millon/lluvia).
  4. Publica UN mensaje en el tópico `resultados-sorteo` con el batch completo.
  5. Crea el tópico si no existe (AdminClient).

Conexión Kafka: listener EXTERNAL → localhost:9094 (fuera del contenedor Docker).

Uso recomendado (cron, lunes 08:00):
  0 8 * * 1 /usr/bin/python3 /opt/pipelines/kafka_publisher_euromillones.py >> /var/log/euromillones_publisher.log 2>&1

Dependencias:
  pip install confluent-kafka playwright
  playwright install chromium
"""

import json
import logging
import sys
import uuid
from datetime import date, timedelta, datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:9094"          # Listener EXTERNAL del broker Docker
TOPIC_NAME      = "resultados-sorteo"
NUM_PARTITIONS  = 3
REPLICATION_F   = 1                         # Single-broker
GAME_ID         = "EMIL"

PRODUCER_CONFIG = {
    "bootstrap.servers":    KAFKA_BOOTSTRAP,
    "enable.idempotence":   True,           # Exactamente una entrega
    "acks":                 "all",
    "retries":              5,
    "retry.backoff.ms":     500,
    "compression.type":     "lz4",          # Mensajes JSON comprimidos
    "linger.ms":            10,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utilidades de fecha
# ---------------------------------------------------------------------------

def ventana_semana_anterior() -> tuple[date, date]:
    """
    Devuelve (lunes, domingo) de la semana natural anterior al día de hoy.
    Si hoy es lunes 2026-06-15 → devuelve (2026-06-08, 2026-06-14).
    """
    hoy   = date.today()
    lunes = hoy - timedelta(days=hoy.weekday() + 7)
    domingo = lunes + timedelta(days=6)
    return lunes, domingo


def kafka_key(lunes: date) -> bytes:
    """Key determinista: EMIL:<lunes_iso>  →  particionado estable por semana."""
    return f"{GAME_ID}:{lunes.isoformat()}".encode("utf-8")


# ---------------------------------------------------------------------------
# Limpieza del payload del scraper
# ---------------------------------------------------------------------------

CAMPOS_RUIDO = {"contenidosRelacionados"}
CAMPOS_NULLABLE_VACIAR = {"millon", "escrutinio_millon", "lluvia", "escrutinio_lluvia"}


def limpiar_sorteo(sorteo: dict) -> dict:
    """
    Elimina campos de ruido (contenidosRelacionados siempre vacío)
    y campos opcionales nulos para reducir el tamaño del mensaje.
    """
    limpio = {k: v for k, v in sorteo.items() if k not in CAMPOS_RUIDO}
    for campo in CAMPOS_NULLABLE_VACIAR:
        if limpio.get(campo) is None:
            limpio.pop(campo, None)
    return limpio


# ---------------------------------------------------------------------------
# Scraping con Playwright
# ---------------------------------------------------------------------------

def fetch_semana(fecha_inicio_str: str, fecha_fin_str: str) -> list[dict]:
    """
    Reutiliza la sesión Playwright para hacer una única consulta a
    loteriasyapuestas.es y devolver los sorteos del rango indicado.
    """
    resultados = []

    def handle_response(response):
        if "buscadorSorteos" in response.url and GAME_ID in response.url:
            if response.status == 200:
                try:
                    data = response.json()
                    resultados.extend(data)
                except Exception as exc:
                    log.warning("Error parseando respuesta de la API: %s", exc)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        log.info("Navegando a loteriasyapuestas.es...")
        page.goto("https://www.loteriasyapuestas.es/es/resultados/euromillones")
        page.wait_for_timeout(4000)

        page.on("response", handle_response)

        # --- Fecha inicio ---
        campo_desde = page.locator(
            "input[id*='desde'], input[placeholder*='esde'], input[name*='esde']"
        ).first
        campo_desde.click()
        page.keyboard.press("Control+a")
        page.keyboard.press("Delete")
        page.keyboard.type(fecha_inicio_str, delay=150)
        page.keyboard.press("Tab")
        page.wait_for_timeout(500)

        # --- Fecha fin ---
        campo_hasta = page.locator(
            "input[id*='hasta'], input[placeholder*='asta'], input[name*='asta']"
        ).first
        campo_hasta.click()
        page.keyboard.press("Control+a")
        page.keyboard.press("Delete")
        page.keyboard.type(fecha_fin_str, delay=150)
        page.keyboard.press("Tab")
        page.wait_for_timeout(500)

        # --- Buscar ---
        boton = page.locator("a:has-text('Buscar'), button:has-text('Buscar')").first
        boton.click()
        page.wait_for_timeout(4000)

        page.remove_listener("response", handle_response)
        browser.close()

    return resultados


# ---------------------------------------------------------------------------
# Gestión del tópico Kafka
# ---------------------------------------------------------------------------

def asegurar_topico(bootstrap: str, topic: str, partitions: int, replication: int) -> None:
    """Crea el tópico si no existe. No falla si ya existe."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=10)

    if topic in metadata.topics:
        log.info("Tópico '%s' ya existe.", topic)
        return

    log.info("Creando tópico '%s' (%d particiones, RF=%d)...", topic, partitions, replication)
    nuevo = NewTopic(topic, num_partitions=partitions, replication_factor=replication)
    futuros = admin.create_topics([nuevo])

    for t, futuro in futuros.items():
        try:
            futuro.result()
            log.info("Tópico '%s' creado correctamente.", t)
        except Exception as exc:
            log.error("Error creando tópico '%s': %s", t, exc)
            raise


# ---------------------------------------------------------------------------
# Publicación en Kafka
# ---------------------------------------------------------------------------

def delivery_callback(err, msg):
    if err:
        log.error(
            "Error en entrega → tópico=%s partición=%d offset=%s: %s",
            msg.topic(), msg.partition(), msg.offset(), err,
        )
    else:
        log.info(
            "Mensaje entregado → tópico=%s partición=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key().decode(),
        )


def publicar_batch(
    producer: Producer,
    lunes: date,
    domingo: date,
    sorteos: list[dict],
) -> None:
    """
    Construye el envelope y publica un único mensaje en el tópico.

    Estructura del value:
    {
        "pipeline_run_id": "<uuid4>",
        "scrape_timestamp": "<ISO 8601 UTC>",
        "game_id": "EMIL",
        "semana_inicio": "YYYY-MM-DD",
        "semana_fin":    "YYYY-MM-DD",
        "num_sorteos":   <int>,
        "sorteos":       [ {...}, ... ]
    }
    """
    run_id = str(uuid.uuid4())

    envelope = {
        "pipeline_run_id": run_id,
        "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
        "game_id":          GAME_ID,
        "semana_inicio":    lunes.isoformat(),
        "semana_fin":       domingo.isoformat(),
        "num_sorteos":      len(sorteos),
        "sorteos":          [limpiar_sorteo(s) for s in sorteos],
    }

    key   = kafka_key(lunes)
    value = json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    log.info(
        "Publicando mensaje → key=%s | sorteos=%d | pipeline_run_id=%s",
        key.decode(), len(sorteos), run_id,
    )

    producer.produce(
        topic=TOPIC_NAME,
        key=key,
        value=value,
        on_delivery=delivery_callback,
    )
    producer.flush()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    lunes, domingo = ventana_semana_anterior()
    inicio_str = lunes.strftime("%d/%m/%Y")
    fin_str    = domingo.strftime("%d/%m/%Y")

    log.info("=== Publisher Euromillones ===")
    log.info("Ventana de scraping: %s → %s", inicio_str, fin_str)

    # 1. Asegurar tópico
    asegurar_topico(KAFKA_BOOTSTRAP, TOPIC_NAME, NUM_PARTITIONS, REPLICATION_F)

    # 2. Scraping
    log.info("Iniciando scraping...")
    sorteos = fetch_semana(inicio_str, fin_str)
    log.info("Sorteos recuperados: %d", len(sorteos))

    if not sorteos:
        log.warning(
            "No se encontraron sorteos para la semana %s → %s. "
            "No se publica ningún mensaje.",
            inicio_str, fin_str,
        )
        sys.exit(0)

    # 3. Publicar en Kafka
    producer = Producer(PRODUCER_CONFIG)
    publicar_batch(producer, lunes, domingo, sorteos)

    log.info("=== Publicación completada ===")


if __name__ == "__main__":
    main()
