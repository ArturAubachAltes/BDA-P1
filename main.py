import ipywidgets as widgets
from IPython.display import display, HTML, clear_output

from DataCollectors import *
from DataFormatting import *
from DataQuality import *
from DataPreparation import *

def main(data_source_income, landing_zone_income, trusted_zone_income, exploration_zone_income,
         data_source_sales, landing_zone_sales, trusted_zone_sales, exploration_zone_sales,
         data_source_shop, landing_zone_shop, trusted_zone_shop, exploration_zone_shop,
         mapaa, model_predictiuu):
    display('')

    #####################
    ## DATA COLLECTORS ##
    #####################
    datacollectors(income= data_source_income, datasearch= data_source_sales, sales= data_source_shop)

    ##############################
    ## DATA FORMATTING PIPELINE ##
    ##############################
    format_=[]
    if landing_zone_income:
        format_.append("income_data")
        print(f"Exploration Zone de la income_data actualitzada")
    if landing_zone_sales:
        format_.append("sales_data")
        print(f"Exploration Zone de la sales_data actualitzada")
    if landing_zone_shop:
        format_.append("shops_data")    
        print(f"Exploration Zone de la shops_data actualitzada")
    DataFormatting(format_)

    ############################
    ## DATA QUEALITY PIPELINE ##
    ############################
    DataQuality(income=trusted_zone_income, datasearch=trusted_zone_sales, sales=trusted_zone_shop)

    ##############################
    ## DATA PREPARAION PIPELINE ##
    ##############################
    DataExploitation(mapa= mapaa, model_predictiu= model_predictiuu)
    

    print("Valores recibidos:")
    print(f"Income: Data Source={data_source_income}, Landing Zone={landing_zone_income}, Trusted Zone={trusted_zone_income}, Exploration Zone={exploration_zone_income}")
    print(f"Sales: Data Source={data_source_sales}, Landing Zone={landing_zone_sales}, Trusted Zone={trusted_zone_sales}, Exploration Zone={exploration_zone_sales}")
    print(f"Shop: Data Source={data_source_shop}, Landing Zone={landing_zone_shop}, Trusted Zone={trusted_zone_shop}, Exploration Zone={exploration_zone_shop}")
    print(f"Exploration Zone checkboxes: mapa={mapaa}, model_predictiu={model_predictiuu}")
