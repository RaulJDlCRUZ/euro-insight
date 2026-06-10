# SETUP SPARK + DELTA + MINIO + AIRFLOW

Este documento resume la configuración y ejecución de un pipeline Bronze usando Spark, Delta Lake, MinIO (S3) y Airflow en entorno Docker.

### ARRANQUE DEL ENTORNO

- Levantar servicios:
```sh
docker-compose up -d
```

- Ver contenedores:

```sh
docker ps
```

- Importante: `airflow-init` en estado "Exited" es NORMAL (inicialización completada)


### COMPROBAR SPARK + DELTA

Dentro del contenedor spark-master:

```sh
docker exec -it spark-master bash
```

Lanzar pyspark con Delta:

```sh
pyspark \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

Verificar `spark.sparkContext.master` debe ser: `spark://spark-master:7077`

De todas formas, el comando que mejor me ha funcionado es:

```sh
pyspark \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin123" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```

---

### CONFIGURAR MINIO (S3A)

En `pyspark`:

```sh
spark.conf.set("fs.s3a.endpoint", "http://minio:9000")
```

- Ver buckets:

```sh
docker exec -it minio-mc mc ls local
```

- Crear bucket bronze:

```sh
docker exec -it minio-mc mc mb local/bronze
```

- Importante: Spark usa `s3a://<bucket>` → **NO usa alias** (`local/myminio`)

### TEST ESCRITURA EN MINIO

En pyspark:

```sh
spark.range(5).write.mode("overwrite").parquet("s3a://bronze/test_parquet")

spark.read.parquet("s3a://bronze/test_parquet").show()
```

### LIMPIEZA

```sh
docker exec -it minio-mc mc rm -r --force local/bronze

docker exec -it minio-mc mc mb local/bronze
```

> Aplica igual para `local/silver` y `local/gold`.
