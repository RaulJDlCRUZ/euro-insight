from pyspark.sql import SparkSession
from delta import *


spark = (SparkSession.builder 
    .appName("CrearArquitecturaMedallion") 
    .master("spark://spark-master:7077") 
    .config("spark.submit.deployMode", "client") 
    
    # Extensiones Delta 
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    
    # Crea el catálogo como un DeltaCatalog
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "s3a://hospital/")
    
    # estamos usando hive en memoria, el catalogo no persiste entre sesiones
    # Establecer la conexión a minio
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")   
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") 
    .config("spark.jars","/opt/spark/jars/hadoop-aws-3.4.0.jar, /opt/spark/jars/aws-java-sdk-bundle-2.23.19.jar, /opt/spark/jars/delta-spark_2.13-4.0.0.jar, /opt/spark/jars/delta-storage-4.0.0.jar, /opt/spark/jars/antlr4-runtime-4.13.1.jar, /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar, /opt/spark/jars/kafka-clients-3.9.1.jar, /opt/spark/jars/mssql-jdbc-12.10.2.jre11.jar") 
    .getOrCreate())

print("SparkSession creada")
# spark = SparkSession.builer.getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BinaryType
from pyspark.sql import Row


bronze_table_checkpoint="s3a://hospital/bronze/pacientes_checkpoint"

# CREAR TABLA DE checkpoint SI NO EXISTE
schema_processed = StructType([
    StructField("last_lsn", BinaryType(), True)
])
if not DeltaTable.isDeltaTable(spark, bronze_table_checkpoint):
    checkpoint_df = spark.createDataFrame([], schema_processed)
    
    # Crear DataFrame con la fila null
    row_df = spark.createDataFrame([Row(last_lsn=None)], schema_processed)
    
    # Unir (append)
    checkpoint_df = checkpoint_df.union(row_df)
    # Guardar como tabla Delta en S3 y registrar en el catálogo
    checkpoint_df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(bronze_table_checkpoint)