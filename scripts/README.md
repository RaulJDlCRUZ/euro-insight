# Ingesta Bronze Euromillones (Spark + Delta + MinIO)

# Fuentes de datos

## 1. Kaggle ES

Dataset histórico de sorteos procedente de Kaggle.

Formato:

* CSV

Ingesta:

* `ingest_sorteos_raw.py`

Origen:

* `Euromillones-Draw-Data.csv`

Source:

* `kaggle_es`

---

## 2. Kaggle UK

Dataset histórico alternativo procedente de Kaggle.

Formato:

* CSV

Ingesta:

* `ingest_sorteos_raw.py`

Origen:

* `euromillions.csv`

Source:

* `kaggle_uk`

---

## 3. SELAE Puntos de Venta

Exportación de puntos de venta oficiales.

Formato:

* CSV

Ingesta:

* `ingest_poi.py`

Origen:

* `PDI.csv`

Source:

* `selae_poi`

---

## 4. SELAE Histórico Scraping

Datos históricos obtenidos mediante scraping.

Formato original:

* JSON Array

Ejemplo:

```json
[
  {...},
  {...},
  {...}
]
```

Problema detectado:

Spark JSON Reader trabaja mejor con JSONL (JSON Lines), donde cada línea contiene un único objeto JSON.

Formato adaptado:

```json
{"id_sorteo":"..."}
{"id_sorteo":"..."}
{"id_sorteo":"..."}
```

Archivo generado:

* `euromillones_historico_completo.jsonl`

Ingesta:

* `ingest_selae_json.py`

Source:

* `selae_scraping`

---

# Decisiones técnicas relevantes

## Mantener contrato Bronze tipo Kafka

La tabla Bronze de scraping ya estaba diseñada para almacenar eventos crudos equivalentes a mensajes Kafka:

Campos principales:

* raw_json
* kafka_offset
* kafka_partition
* ingestion_ts
* pipeline_run_id

Se decidió mantener dicho contrato y no modificar la estructura Delta existente.

---

## Almacenamiento del scraping

Aunque el origen sea un JSONL estructurado, en Bronze se almacena el documento completo serializado en:

```text
raw_json
```

De esta forma:

* Bronze permanece totalmente cruda.
* No existe modelado de negocio.
* Silver será responsable de interpretar y normalizar los datos.

---

## Conversión JSON → JSONL

Se introdujo una etapa previa para convertir el JSON histórico descargado a JSONL.

Motivación:

* Compatibilidad con Spark.
* Evitar problemas de inferencia de esquema.
* Evitar uso de `_corrupt_record`.
* Simplificar validaciones.

---

# Comandos manuales de prueba

## Kaggle ES

```bash
spark-submit ingest_sorteos_raw.py \
  --input_path data/miguelminambres/Euromillones-Draw-Data.csv \
  --source kaggle_es \
  --pipeline_run_id manual_001
```

## Kaggle UK

```bash
spark-submit ingest_sorteos_raw.py \
  --input_path data/sihantao0626/euromillions.csv \
  --source kaggle_uk \
  --pipeline_run_id manual_002
```

## SELAE POI

```bash
spark-submit ingest_poi.py \
  --input_path /opt/project/data/selae_poi_csv/PDI.csv \
  --source selae_poi \
  --pipeline_run_id manual_test_003
```

## SELAE Scraping

```bash
spark-submit ingest_selae_json.py \
  --input_path /opt/project/data/out/euromillones_historico_completo.jsonl \
  --source selae_scraping \
  --pipeline_run_id manual_test_004
```

---

# Validación de JSONL en Spark

Prueba realizada con éxito:

```python
df = spark.read.json(
    "file:///opt/project/data/out/euromillones_historico_completo.jsonl"
)

df.printSchema()
df.show(5, truncate=False)
```

Resultado:

* Esquema inferido correctamente.
* Lectura satisfactoria.
* Sin uso de `_corrupt_record`.

---

# DAGs Airflow definidos

## create_bronze_tables

Responsabilidad:

* Creación de tablas Delta Bronze.

---

## ingest_kaggle_sorteos

Tasks:

* ingest_kaggle_es
* ingest_kaggle_uk

Ambas reutilizan:

* `ingest_sorteos_raw.py`

---

## ingest_selae_poi

Task:

* ingest_poi

Script:

* `ingest_poi.py`

---

## ingest_selae_scraping_jsonl

Task:

* ingest_scraping_jsonl

Script:

* `ingest_selae_json.py`

---

# Identificación de ejecuciones

Se sustituyeron identificadores manuales por macros de Airflow:

```text
{{ dag.dag_id }}_{{ ts_nodash }}
```

Ejemplo:

```text
ingest_selae_scraping_jsonl_20260608T224500
```

Ventajas:

* Trazabilidad.
* Auditoría.
* Debugging.
* Reproducibilidad.

---

# Arquitectura resultante

Fuentes:

* Kaggle ES
* Kaggle UK
* SELAE POI
* SELAE Scraping

↓

Bronze Delta Lake (MinIO)

↓

Silver (normalización futura)

↓

Gold (analítica y explotación futura)

---

# Artefactos generados

Scripts:

* create_bronze_tables.py
* ingest_sorteos_raw.py
* ingest_poi.py
* ingest_selae_json.py

DAGs:

* create_bronze_tables
* ingest_kaggle_sorteos
* ingest_selae_poi
* ingest_selae_scraping_jsonl

Datos intermedios:

* euromillones_historico_completo.jsonl

Storage:

* Delta Lake sobre MinIO (S3A)

Frameworks:

* Apache Spark 4
* Delta Lake 4
* Apache Airflow
* MinIO
* Docker Compose
* PySpark

---------------
