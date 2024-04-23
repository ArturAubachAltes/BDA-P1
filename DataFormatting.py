from pyspark.sql import SparkSession
import os
import duckdb
import pandas as pd


def DataFormatting(quines_act):

    # Inicialización de Spark
    spark = SparkSession.builder \
        .config("spark.jars", "duckdb.jar") \
        .appName("FormattedZone") \
        .getOrCreate()

    # Conexión a la base de datos DuckDB
    con = duckdb.connect(database='/database.duckdb')

    for data in quines_act:
        directory_path = os.path.join("datalake", data)
        parquet_files = [f for f in os.listdir(directory_path) if f.endswith('.parquet')]

        parquet_path = os.path.join(directory_path, parquet_files[0])
        df = spark.read.parquet(parquet_path)
        
        # Conversión a DataFrame de Pandas
        pandas_df = df.toPandas()

        # Registro del DataFrame como una vista en DuckDB
        con.register(f"{data}_view", pandas_df)

        # Creación de la tabla en la base de datos si no existe
        con.execute(f"CREATE TABLE IF NOT EXISTS {data} AS SELECT * FROM {data}_view")

        # Alternativamente, si la tabla ya podría existir y quieres añadir nuevos datos:
        con.execute(f"INSERT INTO {data} SELECT * FROM {data}_view")
        

    # Cierra la conexión a la base de datos al finalizar
    con.commit()
    con.close()


quines_act = ["income", "shops"]

DataFormatting(quines_act)