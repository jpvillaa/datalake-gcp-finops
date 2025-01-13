# Databricks notebook source
# MAGIC %md
# MAGIC **Transferencia de archivos .parquet de zona temp a raw**

# COMMAND ----------

from pyspark.sql import SparkSession
import os

# Configuración de autenticación
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Workspace/Users/proyectobigdata2024@gmail.com/credentials.json"

# Inicializar Spark y configurar opciones de GCS
spark = SparkSession.builder.appName("GCS-Test").getOrCreate()
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/dbfs/tmp/credentials.json")


# COMMAND ----------

import os
from datetime import datetime
from pyspark.sql import SparkSession
from google.cloud import storage

# Configurar Spark
spark = SparkSession.builder.appName("Databricks-Data-Transfer").getOrCreate()

# Configurar cliente GCS
storage_client = storage.Client()

# Función para transferir datos a 'raw'
def transfer_data_to_raw(tables):
    current_year = datetime.now().year
    current_date = datetime.now().strftime("%Y%m%d")  # Fecha en formato YYYYMMDD
    for table in tables:
        schema_tbl = table.split(".")[0]
        tbl = table.split(".")[1]
        
        # Ruta de los datos en la zona 'temp'
        temp_path = f"gs://datalake-zone-landing/{table}/*.parquet"
        
        # Leer los datos sin esquema explícito (inferir las columnas automáticamente)
        temp_df = spark.read.parquet(temp_path)
        
        # Ruta de destino en la zona 'raw'
        raw_folder_path = f"gs://datalake-zone-raw/{schema_tbl}/{tbl}/year={current_year}"
        
        # Escribir los datos en la carpeta 'raw'
        temp_df.write.mode("overwrite").parquet(raw_folder_path)
        print(f"Datos transferidos a {raw_folder_path}")

# Función para mover archivos de 'temp' a 'history' dentro de 'temp'
def move_files_to_history(tables):
    current_date = datetime.now().strftime("%Y%m%d")  # Fecha en formato YYYYMMDD
    for table in tables:
        schema_tbl = table.split(".")[0]
        tbl = table.split(".")[1]
        
        # Ruta de los datos en la zona 'temp'
        temp_folder = f"{schema_tbl}.{tbl}/"
        history_folder = f"{schema_tbl}.{tbl}/history/{current_date}/"
        
        # Obtener el bucket y la carpeta
        bucket_name = "datalake-zone-landing"
        bucket = storage_client.bucket(bucket_name)
        
        # Listar todos los archivos en la carpeta `temp/{schema_tbl}.{tbl}`
        blobs = bucket.list_blobs(prefix=temp_folder)
        
        # Mover archivos a la carpeta `history`
        for blob in blobs:
            if not blob.name.endswith("/") and not blob.name.split("/")[-1].startswith("_"):  # Omitir carpetas y archivos de control
                # Crear la nueva ruta en 'history'
                new_blob_name = blob.name.replace(temp_folder, history_folder, 1)
                # Copiar el archivo a la nueva ubicación
                bucket.copy_blob(blob, bucket, new_blob_name)
                # Eliminar el archivo original
                blob.delete()
                print(f"Archivo {blob.name} movido a {new_blob_name}")
            else:
                print(f"Archivo {blob.name} omitido (archivo de control).")

# Función para eliminar archivos de control en las carpetas especificadas
def delete_control_files(folder_paths):
    """
    Elimina archivos de control (_SUCCESS, _committed_*, _started_*) de las carpetas especificadas.

    :param folder_paths: Lista de rutas de carpetas en las que se buscarán archivos de control.
    """
    for folder_path in folder_paths:
        bucket_name, folder_prefix = folder_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        
        # Listar todos los archivos en la carpeta
        blobs = bucket.list_blobs(prefix=folder_prefix)
        
        # Eliminar archivos de control
        for blob in blobs:
            filename = blob.name.split("/")[-1]
            if filename.startswith("_SUCCESS") or filename.startswith("_committed") or filename.startswith("_started"):
                blob.delete()
                print(f"Archivo de control {blob.name} eliminado.")

# Función para obtener las rutas de carpetas para limpiar archivos de control
def get_folder_paths(tables):
    current_year = datetime.now().year
    current_date = datetime.now().strftime("%Y%m%d")
    folder_paths = []

    for table in tables:
        schema_tbl = table.split(".")[0]
        tbl = table.split(".")[1]
        
        # Carpeta en 'temp'
        temp_folder = f"gs://datalake-zone-landing/{schema_tbl}.{tbl}"
        folder_paths.append(temp_folder)
        
        # Carpeta en 'raw'
        raw_folder = f"gs://datalake-zone-raw/{schema_tbl}/{tbl}/year={current_year}"
        folder_paths.append(raw_folder)
    
    return folder_paths

if __name__ == "__main__":
    # Lista de tablas a procesar
    tables = [
        "production.productcosthistory",
        "production.transactionhistory",
        "production.transactionhistoryarchive",
        "production.productsubcategory",
        "production.productcategory",
        "production.productdescription",
        "production.product"
    ]
    
    # Transferir datos a 'raw'
    print("Iniciando transferencia a 'raw'...")
    transfer_data_to_raw(tables)
    
    # Mover archivos a 'history'
    print("Iniciando transferencia a 'history'...")
    move_files_to_history(tables)
    
    # Eliminar archivos de control
    print("Iniciando limpieza de archivos de control...")
    folder_paths = get_folder_paths(tables)
    delete_control_files(folder_paths)


# COMMAND ----------

df = spark.read.parquet("gs://bigdataeia2024-datalake/temp/production.product/production.product_20241123.parquet")
df.printSchema()

# COMMAND ----------

# Función para eliminar la carpeta 'temp' en GCS
def delete_temp_folder(bucket_name, folder_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        blob.delete()
    print(f"Carpeta '{folder_path}' eliminada correctamente del bucket '{bucket_name}'.")

# Función para verificar y eliminar carpetas 'temp' después de la transferencia
def check_and_delete_temp_folders(bucket_name, tables):
    client = storage.Client()
    for table in tables:
        folder_path = f"temp/{table}"
        control_file = f"{folder_path}/control_file.txt"
        
        # Verificar si el archivo de control existe
        bucket = client.get_bucket(bucket_name)
        control_blob = bucket.blob(control_file)
        
        if control_blob.exists():
            delete_temp_folder(bucket_name, folder_path)
            print(f"Archivo de control encontrado y carpeta '{folder_path}' eliminada.")
        else:
            print(f"No se encontró el archivo de control en '{folder_path}'. La carpeta no se eliminará.")

# Ejecución principal
if __name__ == "__main__":
    tables = [
        "production.productcosthistory",
        "production.transactionhistory",
        "production.transactionhistoryarchive"
    ]
    
    transfer_data_to_raw(tables)
    check_and_delete_temp_folders("bigdata2024-datalake", tables)