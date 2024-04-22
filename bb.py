from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder\
    .config("spark.jars", "duckdb.jar") \
    .getOrCreate()

# Definir el path a la carpeta que contiene los archivos .parquet
formatted_zone_path = "Formatted_Zone/income_data.parquet"

# Leer todos los archivos parquet en la carpeta con Spark
df = spark.read.parquet(formatted_zone_path)

# Hacer algo con el DataFrame, como mostrar las primeras filas
df.show()


# No olvides detener la SparkSession cuando hayas terminado
spark.stop()
