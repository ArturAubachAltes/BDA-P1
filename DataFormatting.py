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
# directory_path_shops = "datalake/shops_data/"

# Llista tots els fitxers en el directori especificat i selecciona el primer CSV
files_income = os.listdir(directory_path_income)
csv_files = [file for file in files_income if file.endswith('.csv')]
first_csv_sales = csv_files[0] if csv_files else None

files_sales = os.listdir(directory_path_sales)
csv_files = [file for file in files_sales if file.endswith('.csv')]
first_csv_file_sales = csv_files[0] if csv_files else None

# files_shops = os.listdir(directory_path_shops)
# csv_files = [file for file in files_shops if file.endswith('.csv')]
# first_csv_file_shops = csv_files[0] if csv_files else None


# Camí complet al primer fitxer CSV
income_path = os.path.join(directory_path_income, first_csv_sales)
income_sales = os.path.join(directory_path_sales, first_csv_file_sales)
# income_shop = os.path.join(directory_path_shops, first_csv_file_shops)



# Carreguem les dades, especificant que la primera fila és la capçalera
income = spark.read.option("header", "true").csv(income_path)
sales = spark.read.option("header", "true").csv(income_path)
# shop = spark.read.option("header", "true").csv(income_path)




# Mostrar les dades
income.show()
sales.show()
# shop.show()
