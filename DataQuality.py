# quality pipeline de les tres taules en duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def DataQuality():
    #obrim sessió spark i duckdb
    spark = SparkSession.builder \
            .config("spark.jars", "./duckdb.jar") \
            .appName("FormattedZone") \
            .getOrCreate()
    
    ###############
    ## AUXILIARS ##
    ###############
    invalid_chars = [' ', ';', '{', '}', '(', ')', '\n', '\t', '=']

    # Función para limpiar los nombres de las columnas reemplazando los caracteres no válidos
    def clean_column_name(column_name):
        for invalid_char in invalid_chars:
            column_name = column_name.replace(invalid_char, "_")  # Reemplaza por subrayado o cualquier otro caracter válido que prefieras
        return column_name

    ############
    ## INCOME ##
    ############
    #carregar taula de duckdb
    income_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
        .option("dbtable", "income_data") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()
    
    # aplicar nateja noms columna
    cleaned_income = income_df.select([col(c).alias(clean_column_name(c)) for c in income_df.columns])

    # guardem taula
    income_df.write \
        .format("jdbc") \
        .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
        .option("dbtable", "cleaned_income") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .mode("overwrite") \
        .save()
    

    ###########
    ## SALES ##
    ###########
    #carregar taula de duckdb
    sales_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
        .option("dbtable", "sales_data") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()

    # aplicar nateja noms columna
    cleaned_sales = sales_df.select([col(c).alias(clean_column_name(c)) for c in sales_df.columns])

    # preprocessing
    sales_usa = cleaned_sales.filter(col("Country_/_Region") == "United States of America") # filtrar EEUU
    sales_usa = sales_usa.dropDuplicates(subset=[col for col in sales_usa.columns if col != "row"]) # eliminar row
    sales_usa = sales_usa.drop("Customer_Name") # eliminar customer_name

    # no fa falta fer imputació de missings perque quan filtrem per USA no ens queden columnes amb missings
    sales_usa = sales_usa.dropna(subset=["Postal_Code"]) # eliminar missings a postal_code
    sales_usa = sales_usa.dropna(subset=["SubRegion"]) # eliminar missings a subregions
    sales_usa = sales_usa.dropna(how="all") # eliminar files que missing a totes les columnes

    # guardem taula
    sales_df.write \
        .format("jdbc") \
        .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
        .option("dbtable", "sales_usa") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .mode("overwrite") \
        .save()


    ###########
    ## SHOPS ##
    ###########
    #carregar taula de duckdb
    shops_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
        .option("dbtable", "shops_data") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()

    # aplicar nateja noms columna
    cleaned_shops = shops_df.select([col(c).alias(clean_column_name(c)) for c in shops_df.columns])
    
    pass
