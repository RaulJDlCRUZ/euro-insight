import psycopg2
from psycopg2 import sql
from psycopg2.errors import DuplicateDatabase

# ==========================================================
# CONFIGURACIÓN
# ==========================================================

DB_CONFIG = {
    "host": "postgres",      # nombre del servicio Docker
    "port": 5432,
    "database": "airflow",   # BD existente para administración
    "user": "airflow",
    "password": "airflow",
}

TARGET_DATABASE = "loterias"

# ==========================================================
# DDL TABLAS
# ==========================================================

TABLES_SQL = [

    # ------------------------------------------------------
    # Provincias
    # ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS provincias_demograficas (
        codigo_provincia VARCHAR(2) PRIMARY KEY,
        nombre_provincia VARCHAR(100),
        poblacion_total INTEGER,
        densidad_poblacion DOUBLE PRECISION,
        renta_media_hogar NUMERIC(10,2),
        tasa_paro DOUBLE PRECISION,
        ccaa VARCHAR(50)
    );
    """,

    # ------------------------------------------------------
    # Administraciones de lotería
    # ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS administraciones_loteria (
        id_administracion VARCHAR(20) PRIMARY KEY,
        nombre_comercial VARCHAR(200),
        direccion VARCHAR(300),
        codigo_postal VARCHAR(5),
        municipio VARCHAR(100),
        codigo_provincia VARCHAR(2),
        latitud NUMERIC(10,8),
        longitud NUMERIC(11,8),
        tipo_establecimiento VARCHAR(50),
        fecha_actualizacion DATE,

        CONSTRAINT fk_administracion_provincia
            FOREIGN KEY (codigo_provincia)
            REFERENCES provincias_demograficas(codigo_provincia)
    );
    """,

    # ------------------------------------------------------
    # Gasto provincial
    # ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS gasto_loteria_provincial (
        codigo_provincia VARCHAR(2),
        anio INTEGER,
        trimestre INTEGER,
        gasto_total_euromillones NUMERIC(12,2),
        num_boletos_vendidos INTEGER,
        gasto_per_capita NUMERIC(8,2),

        PRIMARY KEY (codigo_provincia, anio, trimestre),

        CONSTRAINT fk_gasto_provincia
            FOREIGN KEY (codigo_provincia)
            REFERENCES provincias_demograficas(codigo_provincia)
    );
    """,

    # ------------------------------------------------------
    # Sorteos históricos validados
    # ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS sorteos_historicos_validados (
        id SERIAL PRIMARY KEY,

        fecha_sorteo DATE NOT NULL,

        numeros VARCHAR(50) NOT NULL,
        estrellas VARCHAR(20),

        recaudacion NUMERIC(15,2),

        pais_origen VARCHAR(100),

        validado BOOLEAN NOT NULL DEFAULT FALSE,

        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,

    # ------------------------------------------------------
    # Control de pipelines
    # ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS pipeline_control (
        pipeline_id VARCHAR(100) PRIMARY KEY,

        last_run TIMESTAMP,

        status VARCHAR(50),

        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
]

# ==========================================================
# ÍNDICES
# ==========================================================

INDEXES_SQL = [

    # administraciones
    """
    CREATE INDEX IF NOT EXISTS idx_admin_provincia
    ON administraciones_loteria(codigo_provincia);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_admin_municipio
    ON administraciones_loteria(municipio);
    """,

    # gasto provincial
    """
    CREATE INDEX IF NOT EXISTS idx_gasto_anio
    ON gasto_loteria_provincial(anio);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_gasto_anio_trimestre
    ON gasto_loteria_provincial(anio, trimestre);
    """,

    # sorteos
    """
    CREATE INDEX IF NOT EXISTS idx_sorteos_fecha
    ON sorteos_historicos_validados(fecha_sorteo);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_sorteos_validado
    ON sorteos_historicos_validados(validado);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_sorteos_pais
    ON sorteos_historicos_validados(pais_origen);
    """,

    # pipeline control
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_status
    ON pipeline_control(status);
    """
]

# ==========================================================
# FUNCIONES
# ==========================================================

# def database_exists():
#     """
#     Comprueba si la base de datos destino existe.
#     """
#     conn = psycopg2.connect(**DB_CONFIG)
#     conn.autocommit = True

#     try:
#         with conn.cursor() as cur:
#             cur.execute(
#                 """
#                 SELECT 1
#                 FROM pg_database
#                 WHERE datname = %s
#                 """,
#                 (TARGET_DATABASE,),
#             )

#             return cur.fetchone() is not None

#     finally:
#         conn.close()


def create_database():
    """
    Crea la base de datos si no existe.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT 1
                FROM pg_database
                WHERE datname = %s
                """,
                (TARGET_DATABASE,),
            )

            if cur.fetchone():
                print(
                    f"[INFO] Base de datos '{TARGET_DATABASE}' ya existe."
                )
                return

            try:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(TARGET_DATABASE)
                    )
                )

                print(
                    f"[INFO] Base de datos '{TARGET_DATABASE}' creada."
                )

            except DuplicateDatabase:
                print(
                    f"[INFO] Base de datos '{TARGET_DATABASE}' creada por otro proceso."
                )

    finally:
        conn.close()


def create_tables():
    """
    Crea tablas e índices.
    """
    target_config = DB_CONFIG.copy()
    target_config["database"] = TARGET_DATABASE

    conn = psycopg2.connect(**target_config)

    try:
        with conn.cursor() as cur:

            print("[INFO] Creando/verificando tablas...")

            for statement in TABLES_SQL:
                cur.execute(statement)

            print("[INFO] Creando/verificando índices...")

            for statement in INDEXES_SQL:
                cur.execute(statement)

        conn.commit()

        print(
            "[INFO] Tablas e índices creados/verificados correctamente."
        )

    finally:
        conn.close()


def initialize_database():
    """
    Punto de entrada principal.
    """
    print("[INFO] Inicializando base de datos...")

    create_database()
    create_tables()

    print("[INFO] Inicialización completada.")


# ==========================================================
# EJECUCIÓN DIRECTA
# ==========================================================

if __name__ == "__main__":
    initialize_database()
