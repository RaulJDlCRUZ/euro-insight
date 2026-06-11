"""
================================================================================
PIPELINE SILVER — Paso 2/2: Carga en PostgreSQL (Pandas + psycopg2)
Origen  : s3a://silver/ine_provincias_tmp  (CSV generado por silver_ine_transform.py)
Destino : PostgreSQL → provincias_demograficas
Lógica  : upsert por codigo_provincia (INSERT ... ON CONFLICT DO UPDATE)
Ejecutar después de: silver_ine_transform.py
================================================================================
"""

import csv
import glob
import psycopg2
from psycopg2.extras import execute_values


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

# Spark escribe part-00000-*.csv dentro del directorio
LOCAL_SILVER_TMP = "/opt/spark-data/ine_provincias_tmp"

PG_CONFIG = {
    "host":     "postgres",      # nombre del servicio Docker
    "port":     5432,
    "dbname":   "loterias",
    "user":     "airflow",
    "password": "airflow",
}

# ──────────────────────────────────────────────────────────────────────────────
# LECTURA DEL CSV CON csv STDLIB
# ──────────────────────────────────────────────────────────────────────────────

print("\n[1/3] Leyendo CSV desde spark-data...")

csv_files = glob.glob(f"{LOCAL_SILVER_TMP}/part-*.csv")

if not csv_files:
    raise FileNotFoundError(
        f"No se encontró ningún CSV en {LOCAL_SILVER_TMP}. "
        "Ejecuta silver_ine_transform.py primero."
    )

rows = []
with open(csv_files[0], encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

print(f"  [OK] {len(rows)} filas leídas")
for r in rows:
    print(f"    {r}")


# ──────────────────────────────────────────────────────────────────────────────
# UPSERT EN POSTGRESQL
# Los campos nombre_provincia, densidad_poblacion, tasa_paro y ccaa
# no provienen de esta fuente; se dejan a NULL.
# ──────────────────────────────────────────────────────────────────────────────

print("\n[2/3] Cargando en PostgreSQL...")

UPSERT_SQL = """
    INSERT INTO provincias_demograficas (
        codigo_provincia,
        poblacion_total,
        renta_media_hogar
    )
    VALUES %s
    ON CONFLICT (codigo_provincia) DO UPDATE SET
        poblacion_total   = EXCLUDED.poblacion_total,
        renta_media_hogar = EXCLUDED.renta_media_hogar
"""

def to_int(v):
    try:
        return int(float(v)) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None

def to_float(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None

pg_rows = [
    (
        row["codigo_provincia"],
        to_int(row["poblacion_total"]),
        to_float(row["renta_media_hogar"]),
    )
    for row in rows
]

with psycopg2.connect(**PG_CONFIG) as conn:
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, pg_rows)
    conn.commit()

print(f"  [OK] {len(pg_rows)} provincias cargadas en provincias_demograficas")


# ──────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN
# ──────────────────────────────────────────────────────────────────────────────

print("\n[3/3] Verificando resultado en PostgreSQL...")

VERIFY_SQL = """
    SELECT
        COUNT(*)                                   AS total_provincias,
        COUNT(poblacion_total)                     AS con_poblacion,
        COUNT(renta_media_hogar)                   AS con_renta,
        SUM(poblacion_total)                       AS poblacion_espana,
        ROUND(AVG(renta_media_hogar)::numeric, 2)  AS renta_media_nacional
    FROM provincias_demograficas
"""

with psycopg2.connect(**PG_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute(VERIFY_SQL)
        cols   = [d[0] for d in cur.description]
        result = dict(zip(cols, cur.fetchone()))

print("\n  Resumen provincias_demograficas:")
for k, v in result.items():
    print(f"    {k}: {v}")

print("\n[OK] silver_ine_load completado")
