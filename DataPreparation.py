
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number, first
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when, first
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg



def DataExploitation(mapa:bool= False, model_predictiu:bool= False):
    spark = SparkSession.builder \
        .config("spark.jars", "./duckdb.jar") \
        .appName("ExploitationZone") \
        .getOrCreate()
    
    ##########
    ## MAPA ##
    ##########
    if mapa:
        ############
        ## INCOME ##
        ############
        income = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_income") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load()

        income_selected = income.select("ZIPCODE", "Total_income_amount")

        ###########
        ## SHOPS ##
        ###########
        shops = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_shops") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load()

        shops_selected = shops.select("ZIPCODE", "latitude", "longitude")

        ###########
        ## TAULA ##
        ###########
        # taula a partir de income
        df_model = income_selected

        # renombrem columnes
        df_model = df_model.withColumnRenamed("Total_income_amount", "avg_income_per_zipcode")

        # fem join de taula df_model amb taula shops_selected mitjançant el zipcode
        joined_data = df_model.join(shops_selected, (df_model["ZIPCODE"] == shops_selected["ZIPCODE"]), "inner")
        selected_columns = [df_model[col] for col in df_model.columns] + [shops_selected[col] for col in shops_selected.columns if col not in ["ZIPCODE"]]
        joined_data = joined_data.select(selected_columns)

        # guardem i creem la taula mapa_visualització dins d'un nou fitxer duckdb anomenat exploitation_zone
        joined_data.write \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:exploitation_zone.duckdb") \
            .option("dbtable", "mapa_visualitzacio") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()
        
        print("Exploitation zone de MAPA feta!")
    
    #####################
    ## MODEL PREDICTIU ##
    #####################
    if model_predictiu:
        ############
        ## INCOME ##
        ############


        average_income = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_income") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load() \
            .groupBy("ZIPCODE") \
            .agg(avg("Total_income_amount").alias("Average_Income"))


        ###########
        ## SALES ##
        ###########
        from pyspark.sql.functions import col


        sales_usa = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_sales") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load() \
            .filter(col("Country_/_Region") == "United States of America") \
            .drop("Customer_Name") \
            .select("ZIPCODE", "Category", "Sales", "Order_Quantity")

        # Ventana para contar las categorías más frecuentes por código postal
        category_window = Window.partitionBy("ZIPCODE").orderBy(col("count").desc())
        df_category = sales_usa.groupBy("ZIPCODE", "Category").count()
        df_category = df_category.withColumn("rn", row_number().over(category_window))

        # Filtrar solo las top 5 categorías y pivotear
        df_category = df_category.filter(col("rn") <= 5)
        pivot_category = df_category.groupBy("ZIPCODE").pivot("rn", [1, 2, 3]).agg(first("Category"))

        # Renombrar las columnas para que sean más descriptivas
        b = pivot_category.select(
            col("ZIPCODE"),
            col("1").alias("Category_mas_comun_1"),
            col("2").alias("Category_mas_comun_2"),
            col("3").alias("Category_mas_comun_3")
        )

        df_averages = sales_usa.groupBy("ZIPCODE").agg(
            mean("Sales").alias("Average_Sales"),
            mean("Order_Quantity").alias("Average_Order_Quantity")
        )


        result_df = b.join(df_averages, "ZIPCODE", "outer")

        ###########
        ## SHOP ##
        ###########

        # Cargar los datos y seleccionar columnas requeridas en un paso
        shops = spark.read.format("jdbc") \
            .option("url", "jdbc:duckdb:trusted_zone.duckdb") \
            .option("dbtable", "cleaned_shops") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .load() \
            .select("ZIPCODE", "shop")

        # Calcular la cantidad de cada tipo de tienda por código postal y seleccionar las 5 principales
        window_count = Window.partitionBy("ZIPCODE").orderBy(col("count").desc())
        top_shops = shops.groupBy("ZIPCODE", "shop").count() \
            .withColumn("row_num", row_number().over(window_count)) \
            .filter(col("row_num") <= 5) \
            .drop("row_num") \
            .orderBy("ZIPCODE")

        # Pivoteamos el DataFrame para tener cada tienda en una columna separada
        window_pivot = Window.partitionBy("ZIPCODE").orderBy("count")
        pivot_df = top_shops.withColumn("rn", row_number().over(window_pivot)) \
            .groupBy("ZIPCODE").pivot("rn", [1, 2, 3, 4, 5]).agg(first("shop"))

        # Renombrar las columnas para hacerlas más descriptivas
        pivot_df = pivot_df.select(
            col("ZIPCODE").alias("ZIPCODE"),
            col("1").alias("shop_mas_comun_1"),
            col("2").alias("shop_mas_comun_2"),
            col("3").alias("shop_mas_comun_3"),
            col("4").alias("shop_mas_comun_4"),
            col("5").alias("shop_mas_comun_5")
        )

        # Mostrar el resultado final
        # Unir average_income con result_df
        joined_df = average_income.join(result_df, on="ZIPCODE", how="outer")

        # Unir el resultado anterior con pivot_df
        final_df = joined_df.join(pivot_df, on="ZIPCODE", how="outer")
        final_df.show()

        final_df.write \
            .format("jdbc") \
            .option("url", "jdbc:duckdb:exploitation_zone.duckdb") \
            .option("dbtable", "model_predictiu") \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()

    spark.stop()


#DataExploitation(mapa= True, model_predictiu= True)