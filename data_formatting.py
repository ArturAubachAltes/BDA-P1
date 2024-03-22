'''
Data Formatting Pipline: pasar de LandingZone a FormattedZone
!pip install pyspark
'''
from pyspark.sql import SparkSession

def DataFormatting(income = False, sales = False, shops = False):
  # inicialitzem SparkSession
  if not 'spark' in globals():
    spark = SparkSession.builder.getOrCreate()

  # especifiquem paths
  sales_path = "/datalake/sales_data/"
  income_path = "/datalake/income_data/"
  shops_path = "/datalake/shops_data/"

  # llegim els fitxers json on tenim emmagatzemades les nostres dades
  # creem els dataframes a SparkSQL amb els quals treballarem
  shops_data = spark.read.option("basePath", shops_path).json(shops_path)
  income_data = spark.read.option("basePath", income_path).csv(income_path)
  sales_data = spark.read.option("basePath", sales_path).csv(sales_path)

  return(shops_data, income_data, sales_data)
