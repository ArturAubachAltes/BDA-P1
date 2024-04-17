'''
Data Formatting Pipline: pasar de LandingZone a FormattedZone
!pip install pyspark
'''
from pyspark.sql import SparkSession
import os


# Inicialitzem SparkSession
if not 'spark' in globals():
  spark = SparkSession.builder.getOrCreate()

# Directori on es troben els fitxers CSV
directory_path_income = "datalake/income_data/"
directory_path_sales = "datalake/sales_data/"
directory_path_shops = "datalake/shops_data/"

# Llista tots els fitxers en el directori especificat i selecciona el primer CSV
files_income = os.listdir(directory_path_income)
parquet_files = [file for file in files_income if file.endswith('.parquet')]
first_parquet_file_income = parquet_files[0] if parquet_files else None

files_sales = os.listdir(directory_path_sales)
parquet_files = [file for file in files_sales if file.endswith('.parquet')]
first_parquet_file_sales = parquet_files[0] if parquet_files else None

files_shops = os.listdir(directory_path_shops)
parquet_files = [file for file in files_shops if file.endswith('.parquet')]
first_parquet_file_shops = parquet_files[0] if parquet_files else None


# Camí complet al primer fitxer CSV
income_path = os.path.join(directory_path_income, first_parquet_file_income)
sales_path = os.path.join(directory_path_sales, first_parquet_file_sales)
shop_path = os.path.join(directory_path_shops, first_parquet_file_shops)



# Carreguem les dades, especificant que la primera fila és la capçalera
income = spark.read.option("header", "true").parquet(income_path)
sales = spark.read.option("header", "true").parquet(sales_path)
shop = spark.read.option("header", "true").parquet(shop_path)




# Mostrar les dades
income.show()
sales.show()
shop.show()
