from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
from outliers_data_Q import *


def DataOutliers():
    #obrim sessió spark i duckdb
    spark = SparkSession.builder \
            .config("spark.jars", "./duckdb.jar") \
            .appName("FormattedZone") \
            .getOrCreate()
    
    ############
    ## INCOME ##
    ############
    #carregar taula de duckdb
    income = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
        .option("dbtable", "income_data") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()
    
    categorical_columns = ["STATE"] #variable categorica
    numerical_columns = ["ZIPCODE", "Number of returns", "Adjusted gross income", "Avg AGI",
                        "Number of returns with total income", "Total income amount",
                        "Avg total income", "Number of returns with taxable income",
                        "Taxable income amount", "Avg taxable income"]
    
    plots_cate(income, categorical_columns)
    plots_num(income, numerical_columns)

    