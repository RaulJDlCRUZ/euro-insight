# Bronze → Silver: Pipeline de normalización y validación de sorteos

## Objetivo

Este documento describe el diseño funcional y técnico del pipeline encargado de transformar los datos de la capa **Bronze** a la capa **Silver** del lakehouse basado en Delta Lake.

El objetivo del pipeline es convertir los registros en bruto procedentes de las distintas fuentes CSV en un esquema canónico validado, listo para su explotación analítica y para alimentar las capas superiores del sistema.

---

## Arquitectura general

El pipeline está implementado como un **job batch incremental en PySpark**, ejecutable mediante `spark-submit` y orquestable desde Apache Airflow.

El mismo artefacto debe poder ejecutarse tanto:

* Manualmente desde línea de comandos:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  ... \
  /opt/spark-apps/bronze_to_silver_sorteos.py
```

* Desde un DAG de Airflow mediante un `BashOperator` que invoque el mismo comando.

No existen diferencias funcionales entre ambas modalidades de ejecución.

---

## Flujo de procesamiento

El pipeline implementa las siguientes etapas:

1. Leer el estado del pipeline desde `pipeline_control`.
2. Leer de `bronze.sorteos_raw` únicamente los registros nuevos.
3. Separar los registros según su origen (`source`).
4. Aplicar el mapa de normalización correspondiente a cada origen.
5. Unificar ambos conjuntos de datos en un esquema común.
6. Añadir metadatos y columnas auxiliares.
7. Aplicar reglas de validación.
8. Separar registros válidos e inválidos.
9. Actualizar `silver.sorteos_validados` mediante `MERGE`.
10. Almacenar los registros inválidos en `silver.sorteos_quarantine`.
11. Actualizar el estado del pipeline en `pipeline_control`.

El flujo lógico puede representarse de la siguiente forma:

```text
                    bronze.sorteos_raw
                            │
            filtro incremental (pipeline_control)
                            │
                  separación por source
                  ┌─────────┴─────────┐
                  │                   │
              source=ES           source=UK
                  │                   │
        mapping columnas ES   mapping columnas UK
                  │                   │
                  └─────────┬─────────┘
                            │
                    unionByName()
                            │
                  esquema Silver común
                            │
                     validaciones
                  ┌─────────┴─────────┐
                  │                   │
             registros OK       registros KO
                  │                   │
         MERGE sorteos_validados   append quarantine
                  │                   │
                  └─────────┬─────────┘
                            │
                 actualizar pipeline_control
```

---

## Modelo de lectura de Bronze

Cada fila de `bronze.sorteos_raw` representa un **registro individual del CSV original**.

No se almacena el fichero completo como una única cadena de texto, sino una fila por cada sorteo. Esto permite procesar y derivar individualmente los registros inválidos hacia la capa de cuarentena.

Esquema relevante:

| Campo             | Descripción                                      |
| ----------------- | ------------------------------------------------ |
| `raw_content`     | Registro original del CSV                        |
| `source`          | Identificador del origen (`ES` o `UK`)           |
| `ingestion_ts`    | Timestamp de ingestión en Bronze                 |
| `pipeline_run_id` | Identificador de la ejecución que generó el dato |
| `anio`, `mes`     | Campos auxiliares de particionado                |

---

## Procesamiento incremental

El pipeline no reprocesa toda la capa Bronze en cada ejecución.

Antes de iniciar el procesamiento consulta la tabla `pipeline_control` y recupera el valor de `last_success_ts` asociado al pipeline `bronze_to_silver_sorteos`.

La lectura de Bronze se limita a los registros que cumplan:

```text
ingestion_ts > last_success_ts
```

De esta forma:

* el procesamiento es incremental;
* las ejecuciones son idempotentes;
* el mismo job puede ejecutarse manualmente o mediante Airflow sin cambios.

---

## Estrategia de normalización

Se implementa un único pipeline para todas las fuentes, diferenciando el tratamiento mediante el campo `source`.

### Dataset ES (Minambres)

| Campo Silver   | Campo original       |
| -------------- | -------------------- |
| `fecha_sorteo` | `Date`               |
| `numero_1`     | `N1`                 |
| `estrella_1`   | `LS1`                |
| `recaudacion`  | `Recaudacion_Espana` |

### Dataset UK (Tao)

| Campo Silver   | Campo original         |
| -------------- | ---------------------- |
| `fecha_sorteo` | `DrawDate`             |
| `numero_1`     | `Ball 1`               |
| `estrella_1`   | `Lucky Star 1`         |
| `recaudacion`  | No disponible (`NULL`) |

Cada subconjunto se transforma mediante su propio mapa de correspondencia y posteriormente ambos se unifican mediante `unionByName()` sobre un esquema canónico común.

Este enfoque facilita la incorporación de nuevas fuentes en el futuro sin modificar la lógica global del pipeline.

---

## Reglas de validación

El pipeline aplica, como mínimo, las siguientes comprobaciones de calidad:

* `fecha_sorteo` no nula.
* Números principales (`numero_1` ... `numero_5`) en el rango `[1, 50]`.
* Estrellas (`estrella_1`, `estrella_2`) en el rango `[1, 12]`.

Los registros que superen todas las validaciones se consideran aptos para incorporarse a la capa Silver.

---

## Gestión de registros inválidos

Los registros que no superen las validaciones no interrumpen la ejecución del pipeline.

Estos se almacenan en la tabla `silver.sorteos_quarantine`, que actúa como repositorio de errores para su posterior análisis.

Se recomienda almacenar, además del registro original, la siguiente información:

| Campo              | Descripción                       |
| ------------------ | --------------------------------- |
| `raw_record`       | Registro original                 |
| `source`           | Fuente de origen                  |
| `validation_error` | Motivo de rechazo                 |
| `pipeline_run_id`  | Ejecución que procesó el registro |
| `ingestion_ts`     | Timestamp original de ingestión   |
| `quarantined_at`   | Timestamp de envío a cuarentena   |

---

## Estrategia de deduplicación

La carga sobre `silver.sorteos_validados` se realiza mediante una operación `MERGE INTO` de Delta Lake.

La clave de negocio seleccionada para evitar duplicados es:

* `fecha_sorteo`
* `pais_origen`

La condición lógica utilizada será equivalente a:

```text
target.fecha_sorteo = source.fecha_sorteo
AND
target.pais_origen = source.pais_origen
```

Esta estrategia garantiza que una recarga accidental de un fichero histórico no genere registros duplicados.

---

## Tabla `pipeline_control`

El estado del pipeline se mantiene en una tabla Delta dedicada denominada `pipeline_control`, almacenada en la capa Silver.

Su finalidad es proporcionar soporte al procesamiento incremental y a la monitorización de ejecuciones.

Esquema propuesto:

| Campo                  | Tipo      |
| ---------------------- | --------- |
| `pipeline_name`        | STRING    |
| `last_success_ts`      | TIMESTAMP |
| `last_pipeline_run_id` | STRING    |
| `rows_processed`       | BIGINT    |
| `rows_valid`           | BIGINT    |
| `rows_invalid`         | BIGINT    |
| `updated_at`           | TIMESTAMP |

Al finalizar correctamente una ejecución, el pipeline actualiza el registro correspondiente con las métricas y el instante de finalización.

---

## Secuencia completa del pipeline

| Paso | Acción                                                      |
| ---- | ----------------------------------------------------------- |
| 1    | Leer `pipeline_control`                                     |
| 2    | Leer Bronze incremental                                     |
| 3    | Separar registros ES y UK                                   |
| 4    | Aplicar normalización específica por origen                 |
| 5    | Unificar esquemas (`unionByName`)                           |
| 6    | Añadir metadatos auxiliares                                 |
| 7    | Ejecutar validaciones                                       |
| 8    | Separar registros válidos e inválidos                       |
| 9    | Ejecutar `MERGE` sobre `silver.sorteos_validados`           |
| 10   | Insertar registros inválidos en `silver.sorteos_quarantine` |
| 11   | Actualizar `pipeline_control`                               |
| 12   | Finalizar ejecución                                         |


--------

# Pipeline INE — Bronze → Silver

Ingesta y normalización de datos del INE (población municipal y renta por hogar) hacia PostgreSQL, sobre una stack Spark + MinIO + Airflow + PostgreSQL en Docker.

---

## Fuentes

| Dataset | Ficheros | Separador | Encoding |
|---|---|---|---|
| Población municipal | 52 CSVs provinciales en `ine_detalle_municipal_csv/` | `;` | UTF-8 BOM |
| Atlas de renta por hogar | `ine_atlas_distrib_renta_hogares_csv/30824.csv` | `;` | UTF-8 BOM |

---

## Arquitectura

```
Fuentes CSV (local)
       │
       ▼
bronze_ine_ingesta.py        →   s3a://bronze/ine_raw  (Delta Lake, particionado por source)
       │
       ▼
silver_ine_transform.py      →   /opt/spark-data/ine_provincias_tmp/part-00000.csv
       │
       ▼
silver_ine_load.py           →   PostgreSQL → provincias_demograficas
```

---

## Scripts

### `bronze_ine_ingesta.py`
- Ejecutar con: `spark-submit`
- Lee los 52 CSVs provinciales y el fichero de renta línea a línea en chunks de **5.000 líneas** para controlar el heap del driver (2 workers × 2 GB).
- Escribe cada línea como `raw_content` en `bronze.ine_raw` (Delta Lake en MinIO) con los siguientes metadatos de trazabilidad:

| Columna | Descripción |
|---|---|
| `raw_content` | Línea original del CSV sin transformar |
| `source` | `ine_municipal` / `ine_renta_hogares` |
| `file_name` | Nombre del fichero de origen |
| `file_md5` | Hash MD5 del fichero completo |
| `file_encoding` | `utf-8-sig` (UTF-8 con BOM — estándar INE) |
| `ingestion_ts` | Timestamp UTC de ingesta |
| `pipeline_run_id` | UUID único por ejecución |

**Resultado:** 736.972 filas (`ine_municipal`) + 3.009.313 filas (`ine_renta_hogares`).

---

### `silver_ine_transform.py`
- Ejecutar con: `spark-submit`
- Lee `bronze.ine_raw` desde Delta Lake.
- Parsea `raw_content` con dos UDFs:
  - `extract_codigo_provincia` — extrae los 2 primeros dígitos del código INE del campo `Municipios`.
  - `parse_numeric_es` — normaliza el formato numérico español (punto = miles, coma = decimal).
- Agrega a nivel provincia tomando siempre el **año más reciente disponible**:
  - `poblacion_total`: suma de `Total` donde `Sexo = 'Total'`.
  - `renta_media_hogar`: media de `Total` donde `Indicador = 'Renta neta media por hogar'` y sin desglose por distrito ni sección.
- Escribe el resultado con `csv` stdlib desde el driver (evita el `FileOutputCommitter` de Spark y los problemas de rename en volúmenes Docker montados). Idempotente: escritura atómica vía rename `part-00000.tmp → part-00000.csv`.

---

### `silver_ine_load.py`
- Ejecutar con: `python` (contenedor Airflow — tiene `psycopg2`, no tiene PySpark)
- Lee `part-00000.csv` con `csv` stdlib.
- Hace upsert en PostgreSQL con `ON CONFLICT (codigo_provincia) DO UPDATE` — idempotente por diseño.
- Campos cargados: `codigo_provincia`, `poblacion_total`, `renta_media_hogar`.
- Campos dejados a NULL para otros pipelines: `nombre_provincia`, `densidad_poblacion`, `tasa_paro`, `ccaa`.

**Resultado:** 52 provincias cargadas, `poblacion_espana = 98.228.988`, `renta_media_nacional = 33.147,97 €`.

> ⚠️ La cifra de población (~98M) requiere revisión: puede indicar doble conteo en la fuente. Pendiente de validación en pipeline Silver de calidad.

---

### `dag_silver_ine_provincias.py`
DAG de Airflow que encadena los dos pasos Silver:

```python
silver_ine_transform  >>  silver_ine_load
```

`transform` se lanza con `spark-submit`; `load` con `python` a secas. La dependencia garantiza que `load` no se ejecuta si `transform` ha fallado.

---

## Decisiones técnicas relevantes

- **Bronze almacena raw_content sin parsear**: el separador, el formato numérico y la estructura de columnas quedan diferidos a Silver, siguiendo el principio de fidelidad de la capa Bronze.
- **Encoding hardcodeado a `utf-8-sig`**: los CSVs del INE se publican en UTF-8 con BOM. Python gestiona el BOM automáticamente con este alias, sin dependencias externas.
- **Sin pandas en la stack Spark**: toda la lógica de transformación usa PySpark puro. La escritura del CSV intermedio usa `csv` stdlib desde el driver para evitar conflictos de permisos entre contenedores Docker.
- **Separación transform / load**: necesaria porque el contenedor Airflow tiene una versión de PySpark incompatible con el cluster Spark. `transform` corre en el spark-master vía `spark-submit`; `load` corre en Airflow con Python local.
