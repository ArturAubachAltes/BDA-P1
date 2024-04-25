# quality pipeline de les tres taules en duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, pi, atan, exp, when
from uszipcode import SearchEngine


def DataQuality(income:bool= False, datasearch:bool= False, sales:bool= False):
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
    if income:
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
        cleaned_income.write \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_income") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()

        print("Trusted zone de INCOME, actualitzat")
    

    ###########
    ## SALES ##
    ###########

    if sales:
        # Cargar la tabla desde DuckDB
        sales_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
            .option("dbtable", "sales_data") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load()

        # Limpiar nombres de columnas y preprocesamiento
        cleaned_sales = sales_df.select([col(c).alias(clean_column_name(c)) for c in sales_df.columns])
        sales_usa = cleaned_sales.filter(col("Country_/_Region") == "United States of America")
        
        # Eliminar duplicados excepto en la columna 'row', remover columnas no necesarias y eliminar filas con todos los datos missing
        sales_usa = sales_usa.dropDuplicates([c for c in sales_usa.columns if c != "row"]) \
                            .drop("Customer_Name") \
                            .dropna(subset=["Postal_Code", "SubRegion"], how="any") \
                            .dropna(how="all")


        sales_usa = sales_usa.withColumnRenamed("Postal_Code", "ZIPCODE")

        # Guardar el resultado en DuckDB
        sales_usa.write \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_sales") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()

        print("Trusted zone de SALES, actualitzat")

    ###########
    ## SHOPS ##
    ###########
    if datasearch == True:
        #carregar taula de duckdb
        shops = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
            .option("dbtable", "shops_data") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load()

        # canviem nom columnes perque no ens deixa accedir-hi si tenen caracters especials
        # geometry.x
        new_column_name = "geometry_x"
        old_column_name = "geometry.x"
        shops = shops.withColumnRenamed(old_column_name, new_column_name)

        # geometry.y
        new_column_name = "geometry_y"
        old_column_name = "geometry.y"
        shops = shops.withColumnRenamed(old_column_name, new_column_name)

        # attributes.shop
        new_column_name = "shop"
        old_column_name = "attributes.shop"
        shops = shops.withColumnRenamed(old_column_name, new_column_name)

        # attributes.name
        new_column_name = "name"
        old_column_name = "attributes.name"
        shops = shops.withColumnRenamed(old_column_name, new_column_name)

        # attributes.objectid
        new_column_name = "index"
        old_column_name = "attributes.objectid"
        shops = shops.withColumnRenamed(old_column_name, new_column_name)

        # attributes.addr_postcode
        new_column_name = "postcode"
        old_column_name = "attributes.addr_postcode"
        shops = shops.withColumnRenamed(old_column_name, new_column_name)

        R = 6378137 #radi de la terra
        shops = shops.withColumn('Longitude', (col('geometry_x') / lit(R)) * (180 / pi()))\
                    .withColumn('Latitude',  (((pi() / 2) - 2 * atan(exp(-col('geometry_y') / lit(R)))) * (180 / pi())))\


        # ens quedem només amb columnes seleccionades
        selected_columns = ['latitude', 'longitude', "shop", "name", "index", "postcode"]
        shops = shops.select(selected_columns)

        shops = shops.filter(~(col("latitude").isNull() |
                                col("longitude").isNull() |
                                col("shop").isNull() |
                                col("name").isNull() |
                                col("index").isNull()))
        
        #Fer imputacio per zipcode
        #Nomes mires aquelles files on el postcode es nulo
        null_df = shops.filter(col("postcode").isNull())

        collected_data = null_df.select('index').dropDuplicates().collect()

        for i in collected_data:
            #trobar la latitud i longitud
            selected = shops.filter(shops.index == i[0]).dropDuplicates().collect()
            #aplicar la funció
            search = SearchEngine() 
            result = search.by_coordinates(lat=selected[0].latitude, lng=selected[0].longitude, returns=1)
            if result:
                shops = shops.withColumn("postcode", when(shops.index == i[0], result[0].zipcode).otherwise(shops['postcode']))
        
        #Els que no shan pogut imputar s'eliminen
        shops = shops.filter(~(col("postcode").isNull()))


        shops = shops.withColumnRenamed("postcode", "ZIPCODE")
            
        # guardem taula
        shops.write \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_shops") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()
        
        shops.show()
        print("Trusted zone de SHOPS, actualitzat")
    spark.stop()
    
    

#DataQuality(income=True, datasearch=True, sales=True)
