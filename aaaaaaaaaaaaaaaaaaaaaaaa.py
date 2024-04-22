from pyspark.sql import SparkSession
import os
import duckdb
import pandas as pd

# Inicialización de Spark
spark = SparkSession.builder \
    .appName("FormattedZone") \
    .getOrCreate()

# Rutas base
base_path = "datalake"  # Cambia esto por la ruta correcta del data lake
formatted_zone_path = "Formatted_Zone"  # Cambia esto por la ruta correcta de la zona formateada

# Asegurarse de que existe la carpeta Formatted_Zone
if not os.path.exists(formatted_zone_path):
    os.makedirs(formatted_zone_path)

datasets = ["income_data", "sales_data", "shops_data"]

# Función para cargar los datos en DuckDB
def load_into_duckdb(formatted_parquet_path, dataset_name):
    conn = None  # Inicializa la conexión como None
    try:
        # Establecer la conexión con DuckDB
        conn = duckdb.connect(database='./database.duckdb', read_only=False)

        
        # Leer el archivo Parquet en un DataFrame de Pandas
        df = pd.read_parquet(formatted_parquet_path)
        
        # Cargar los datos en DuckDB
        conn.execute(f"DROP TABLE IF EXISTS {dataset_name}")
        conn.execute(f"CREATE TABLE {dataset_name} AS SELECT * FROM df")
        
    except Exception as e:
        print(f"Error al cargar datos en DuckDB: {e}")
    finally:
        # Cerrar la conexión solo si se ha establecido
        if conn:
            conn.close()

# Procesamiento y carga de cada conjunto de datos
for dataset in datasets:
    # Encontrar el primer archivo .parquet en el directorio correspondiente
    directory_path = os.path.join(base_path, dataset)
    parquet_files = [f for f in os.listdir(directory_path) if f.endswith('.parquet')]
    if not parquet_files:
        print(f"No se encontraron archivos .parquet en {directory_path}")
        continue
    
    # Leer el primer archivo .parquet con Spark
    parquet_path = os.path.join(directory_path, parquet_files[0])
    df = spark.read.parquet(parquet_path)
    
    # Guardar el DataFrame procesado en la zona formateada como archivo parquet
    formatted_parquet_path = os.path.join(formatted_zone_path, f"{dataset}.parquet")
    df.write.mode("overwrite").parquet(formatted_parquet_path)
    
    # Cargar los datos formateados en DuckDB
    load_into_duckdb(formatted_parquet_path, dataset)

# Cerrar la sesión de Spark
spark.stop()
