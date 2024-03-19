'''
Data Formatting Pipline: pasar de LandingZone a FormattedZone
!pip install pyspark
'''
from pyspark.sql import SparkSession

# inicialitzem SparkSession
if not 'spark' in globals():
  spark = SparkSession.builder.getOrCreate()

# llegim els fitxers json on tenim emmagatzemades les nostres dades
# creem els dataframes a SparkSQL amb els quals treballarem
shops_data = spark.read.json("/datalake/sales_data/2024-03-19_shops_data.json")
income_data = spark.read.json("/datalake/sales_data/2024-03-19_shops_data.json")
sales_data = spark.read.json("/datalake/sales_data/2024-03-19_shops_data.json")
