from pyspark.sql.functions import col  
import plotly.express as px 

def plots_cate(spark,categorical_columns:list):
    for feature in categorical_columns:
        df_pd = spark.groupBy(feature).count().withColumn('percent', col('count') / spark.count() * 100).toPandas()
        fig1 = px.bar(df_pd, x=feature, y='count', text='percent', color=feature,
                    title=f'Distribución de la variable {feature}',
                    labels={'count': 'Conteo', 'percent': 'Porcentaje'})
        fig1.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
        fig1.show()


def plots_num(spark,numerical_columns:list):
    for feature in numerical_columns:
        df_pd = spark.select(feature).toPandas()
        fig2 = px.histogram(df_pd, x=feature, marginal="box", title=f'Histograma de {feature}')
        fig2.update_layout(xaxis_title=feature, yaxis_title='Conteo')
        fig2.show()

#