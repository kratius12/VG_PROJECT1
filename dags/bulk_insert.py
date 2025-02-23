import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Configuración de logs
LOG_FILE = "bulk_insert.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuración de conexión a SQL Server con pymssql
SERVER = "DESKTOP-FLUCE56"
DATABASE = "Testing"
USERNAME = "Usuario"
PASSWORD = ""

# Crear conexión SQLAlchemy
CONN_STR = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
engine = create_engine(CONN_STR)

# Tamaño del bloque para carga masiva
CHUNK_SIZE = 500000  

def log_event(message):
    """Registra eventos en el log y en la consola."""
    logging.info(message)
    print(message)

def validate_csv_schema(csv_path):
    """Valida el esquema del CSV usando pandas."""
    expected_columns = ["id", "nombre", "edad"]  # Columnas esperadas
    df_sample = pd.read_csv(csv_path, nrows=5)  # Leer solo 5 filas para validación

    if list(df_sample.columns) != expected_columns:
        raise ValueError(f"Error de esquema: Se esperaban {expected_columns}, pero se encontraron {list(df_sample.columns)}")

    log_event("Validación de esquema exitosa.")

def bulk_insert_staging(csv_path):
    """Carga un CSV en la tabla staging en bloques de 500,000 registros usando pymssql para mayor eficiencia."""
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"El archivo {csv_path} no existe.")

        log_event(f"Iniciando carga de {csv_path} en staging...")

        # Validar esquema del CSV
        validate_csv_schema(csv_path)

        df_iter = pd.read_csv(csv_path, chunksize=CHUNK_SIZE)

        # Usar una conexión directa para mejorar el rendimiento
        conn = engine.raw_connection()
        cursor = conn.cursor()

        for i, chunk in enumerate(df_iter):
            # Convertir DataFrame a lista de tuplas para `executemany`
            data_tuples = [tuple(x) for x in chunk.itertuples(index=False, name=None)]
            
            # Crear consulta de inserción masiva
            query = "INSERT INTO staging_table (id, nombre, edad) VALUES (%s, %s, %s)"

            cursor.executemany(query, data_tuples)
            conn.commit()

            log_event(f"Bloque {i+1} cargado en staging ({len(chunk)} registros).")

        cursor.close()
        conn.close()

        log_event("Carga de datos en staging finalizada.")

    except Exception as e:
        log_event(f"Error en carga de datos: {str(e)}")
        raise  # Re-lanzar error para manejo en Airflow
