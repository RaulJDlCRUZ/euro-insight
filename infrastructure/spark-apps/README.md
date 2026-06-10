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
