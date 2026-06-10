import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, nullif, split, trim, to_date, year, month, regexp_replace, regexp_extract, date_format, current_timestamp, dayofweek, coalesce
)

from delta.tables import DeltaTable


# ============================
# Spark Session
# ============================
def create_spark():
    return (
        SparkSession.builder
        .appName("BronzeToSilverSorteos")
        .master("spark://spark-master:7077")
        .config("spark.submit.deployMode", "client")

        # Delta
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # JARs
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.4.0.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar,"
            "/opt/spark/jars/delta-spark_2.13-4.0.0.jar,"
            "/opt/spark/jars/delta-storage-4.0.0.jar"
        )
        .getOrCreate()
    )

def parse_fecha(col_name: str):
    """
    Intenta parsear la columna fecha_sorteo con los dos formatos posibles.
    Devuelve NULL si ninguno funciona (en lugar de explotar).
    """
    return coalesce(
        to_date(col(col_name), "yyyy-MM-dd"),   # UK
        to_date(col(col_name), "M/d/yyyy"),      # ES
        to_date(col(col_name), "d/M/yyyy"),      # variante ES
    )


# ============================
# PIPELINE
# ============================
def run_pipeline(spark, bronze_path, silver_valid, silver_quarantine, pipeline_run_id):

    # ----------------------------
    # 1. Leer Bronze
    # ----------------------------
    df = spark.read.format("delta").load(bronze_path)

    df_es = df.filter(col("source") == "kaggle_es")
    df_uk = df.filter(col("source") == "kaggle_uk")


    # ============================
    # 2. ES parsing
    # ============================
    es_split = split(col("raw_content"), ",")

    es = df_es.select(
        to_date(es_split.getItem(0), "M/d/yyyy").alias("fecha_sorteo"),

        es_split.getItem(4).cast("int").alias("numero_1"),
        es_split.getItem(5).cast("int").alias("numero_2"),
        es_split.getItem(6).cast("int").alias("numero_3"),
        es_split.getItem(7).cast("int").alias("numero_4"),
        es_split.getItem(8).cast("int").alias("numero_5"),

        es_split.getItem(9).cast("int").alias("estrella_1"),
        es_split.getItem(10).cast("int").alias("estrella_2"),

        es_split.getItem(3).cast("double").alias("recaudacion"),

        lit("ES").alias("pais_origen")
    )


    # ============================
    # 3. UK parsing
    # ============================
    # uk_split = split(col("raw_content"), r"\|")

    # main_nums = split(uk_split.getItem(1), ",")
    # stars = split(uk_split.getItem(2), ",")
    # jackpot = uk_split.getItem(4)

    # uk = df_uk.select(
    #     to_date(uk_split.getItem(0)).alias("fecha_sorteo"),

    #     trim(main_nums.getItem(0)).cast("int").alias("numero_1"),
    #     trim(main_nums.getItem(1)).cast("int").alias("numero_2"),
    #     trim(main_nums.getItem(2)).cast("int").alias("numero_3"),
    #     trim(main_nums.getItem(3)).cast("int").alias("numero_4"),
    #     trim(main_nums.getItem(4)).cast("int").alias("numero_5"),

    #     trim(stars.getItem(0)).cast("int").alias("estrella_1"),
    #     trim(stars.getItem(1)).cast("int").alias("estrella_2"),

    #     regexp_replace(jackpot, "[£,]", "").cast("double").alias("recaudacion"),

    #     lit("UK").alias("pais_origen")
    # )


    # raw_content UK tras concat_ws(",") tiene esta forma:
    # 2004-02-13,16, 29, 32, 36, 41,7, 9,,£10143000
    # pos: 0      1   2   3   4   5  6  7  8  9
    # La limpieza _clean_uk_multiline ya eliminó '\nRolled' antes de la ingesta,
    # pero £ y , dentro del jackpot quedan; se limpian con regexp_replace.
    uk_split = split(col("raw_content"), ",")

    uk = df_uk.select(
        to_date(trim(uk_split.getItem(0)), "yyyy-MM-dd").alias("fecha_sorteo"),

        trim(uk_split.getItem(1)).cast("int").alias("numero_1"),
        trim(uk_split.getItem(2)).cast("int").alias("numero_2"),
        trim(uk_split.getItem(3)).cast("int").alias("numero_3"),
        trim(uk_split.getItem(4)).cast("int").alias("numero_4"),
        trim(uk_split.getItem(5)).cast("int").alias("numero_5"),

        trim(uk_split.getItem(6)).cast("int").alias("estrella_1"),
        trim(uk_split.getItem(7)).cast("int").alias("estrella_2"),

        # regexp_replace(
        #     trim(uk_split.getItem(9)), r"[£,]", ""
        # ).cast("double").alias("recaudacion"),

        # regexp_replace(
        #     regexp_extract(col("raw_content"), r"£[\d,\.]+", 0),
        #     r"[£,]", ""
        # ).cast("double").alias("recaudacion"),

        regexp_replace(
            nullif(
                regexp_extract(col("raw_content"), r"£[\d,\.]+", 0),
                lit("")
            ),
            r"[£,]", ""
        ).cast("double").alias("recaudacion"),

        lit("UK").alias("pais_origen")
    )


    # ----------------------------
    # 4. UNION
    # ----------------------------
    df_all = es.unionByName(uk)


    # ----------------------------
    # 5. Enriquecimiento temporal
    # ----------------------------
    df_all = (
        df_all
        .withColumn("anio", year(col("fecha_sorteo")))
        .withColumn("mes", month(col("fecha_sorteo")))
        .withColumn(
            "dia_semana",
            date_format(col("fecha_sorteo"), "EEEE")
        )
        .withColumn(
            "pipeline_run_id",
            lit(pipeline_run_id)
        )
        .withColumn(
            "validated_at",
            current_timestamp()
        )
    )


    # ----------------------------
    # 6. Validación
    # ----------------------------
    valid = df_all.filter(
        (col("fecha_sorteo").isNotNull()) &
        col("numero_1").between(1, 50) &
        col("numero_2").between(1, 50) &
        col("numero_3").between(1, 50) &
        col("numero_4").between(1, 50) &
        col("numero_5").between(1, 50) &
        col("estrella_1").between(1, 12) &
        col("estrella_2").between(1, 12)
    )

    invalid = df_all.subtract(valid)


    # ----------------------------
    # 7. SILVER VALIDATED (MERGE)
    # ----------------------------
    if DeltaTable.isDeltaTable(spark, silver_valid):

        silver_table = DeltaTable.forPath(spark, silver_valid)

        print("=== SCHEMA VALID ===")
        valid.printSchema()

        print("=== COLUMNS VALID ===")
        print(valid.columns)

        silver_table.alias("t").merge(
            valid.alias("s"),
            "t.fecha_sorteo = s.fecha_sorteo AND t.pais_origen = s.pais_origen"
        ).whenNotMatchedInsert(
            values={
                "fecha_sorteo": "s.fecha_sorteo",
                "dia_semana": "s.dia_semana",
                "numero_1": "s.numero_1",
                "numero_2": "s.numero_2",
                "numero_3": "s.numero_3",
                "numero_4": "s.numero_4",
                "numero_5": "s.numero_5",
                "estrella_1": "s.estrella_1",
                "estrella_2": "s.estrella_2",
                "recaudacion": "s.recaudacion",
                "pais_origen": "s.pais_origen",
                "pipeline_run_id": "s.pipeline_run_id",
                "validated_at": "s.validated_at",
                "anio": "s.anio",
                "mes": "s.mes"
            }
        ).execute()

    else:
        valid.write.format("delta").mode("overwrite").save(silver_valid)


    # ----------------------------
    # 8. QUARANTINE
    # ----------------------------
    invalid.write.format("delta").mode("append").save(silver_quarantine)

    print("[OK] Pipeline Bronze → Silver completado")


# ============================
# MAIN
# ============================
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--bronze_path", required=True)
    parser.add_argument("--silver_valid", required=True)
    parser.add_argument("--silver_quarantine", required=True)
    parser.add_argument("--pipeline_run_id", required=True)

    args = parser.parse_args()

    spark = create_spark()

    try:
        run_pipeline(
            spark,
            args.bronze_path,
            args.silver_valid,
            args.silver_quarantine,
            args.pipeline_run_id
        )
    finally:
        spark.stop()
