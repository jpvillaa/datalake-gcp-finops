import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from datetime import datetime
from google.cloud import storage

# Configurar el logging
def setup_logging():
    log_directory = "./logs"
    os.makedirs(log_directory, exist_ok=True)  # Crear el directorio si no existe
    log_filename = f"{log_directory}/execution_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        filename=log_filename,       # Nombre del archivo de log
        level=logging.INFO,          # Nivel de registro (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format='%(asctime)s - %(levelname)s - %(message)s',  # Formato del log
        datefmt='%Y-%m-%d %H:%M:%S'  # Formato de fecha y hora
    )
    return log_filename

def log_and_print(message, level="info"):
    """Loguea y muestra mensajes en consola."""
    if level.lower() == "info":
        logging.info(message)
    elif level.lower() == "error":
        logging.error(message)
    elif level.lower() == "warning":
        logging.warning(message)
    print(message)

def load_query_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def get_tables_from_directory(directory):
    """Listar tablas a partir de los archivos .sql en un directorio."""
    sql_files = [file for file in os.listdir(directory) if file.endswith('.sql')]
    tables = [os.path.splitext(file)[0] for file in sql_files]
    return tables

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Sube un archivo local a Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    log_and_print(f"Archivo subido a GCS: {destination_blob_name}")

def load_data_to_temp(tables, sql_directory, temp_directory, bucket_name, db_connection):
    """Carga los datos a la zona temporal de GCS."""
    log_and_print("Iniciando carga a la zona temporal...")
    os.makedirs(temp_directory, exist_ok=True)

    try:
        engine = create_engine(db_connection)
        log_and_print("Conexión a la base de datos establecida.")
    except Exception as e:
        log_and_print(f"Error al conectar a la base de datos: {e}", level="error")
        return

    for table in tables:
        try:
            query = load_query_from_file(f"{sql_directory}/{table}.sql")
            log_and_print(f"Consultando datos de la tabla {table}...")
            
            df = pd.read_sql(query, engine)

            # Crear tabla de pyarrow y asegurar que los timestamps estén en milisegundos
            table_data = pa.Table.from_pandas(df)

            # Ruta local temporal
            local_temp_path = f"{temp_directory}/{table}_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
            pq.write_table(table_data, local_temp_path, coerce_timestamps='ms')
            log_and_print(f"Archivo temporal creado: {local_temp_path}")

            # Subir a GCS
            destination_blob_name = f"{table}/{table}_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
            upload_to_gcs(bucket_name, local_temp_path, destination_blob_name)

            # Eliminar archivo local
            os.remove(local_temp_path)
            log_and_print(f"Archivo temporal eliminado: {local_temp_path}")
        except Exception as e:
            log_and_print(f"Error procesando la tabla {table}: {e}", level="error")

if __name__ == "__main__":
    # Configurar logging
    log_file_path = setup_logging()
    log_and_print(f"Archivo de log: {log_file_path}")

    # Parámetros configurables
    GOOGLE_APPLICATION_CREDENTIALS = "static/credentials.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

    SQL_DIRECTORY = "sql"
    TEMP_DIRECTORY = "temp"
    BUCKET_NAME = "datalake-zone-landing"
    DB_CONNECTION = "postgresql://postgres:655215@localhost:5432/Adventureworks"

    # Log de inicio
    log_and_print("Inicio del proceso de carga")

    try:
        # Obtener las tablas desde el directorio SQL
        tables = get_tables_from_directory(SQL_DIRECTORY)
        log_and_print(f"Tablas encontradas: {tables}")

        # Ejecutar el proceso de carga
        load_data_to_temp(tables, SQL_DIRECTORY, TEMP_DIRECTORY, BUCKET_NAME, DB_CONNECTION)
    except Exception as e:
        log_and_print(f"Error general en el script: {e}", level="error")

    # Log de finalización
    log_and_print("Fin del proceso de carga")
