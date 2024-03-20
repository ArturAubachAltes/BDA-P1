import os
import re
import json
import requests
from datetime import datetime, timedelta

def descargar_datastsrch():
    today = datetime.now().strftime("%Y-%m-%d")

    # URL de la API para realizar la consulta
    url = 'https://services6.arcgis.com/Do88DoK2xjTUCXd1/arcgis/rest/services/OSM_Shops_NA/FeatureServer/0/query?outFields=*&where=1%3D1&f=json'

    # Ahora compara la fecha más reciente con la fecha de hace 30 días
    response = requests.get(url)
    # Verificar si la solicitud fue exitosa (código de estado 200)

    if response.status_code == 200:
        # Convertir la respuesta en formato JSON a un diccionario de Python
        data = response.json()

        with open(f'./datalake/shops_data/{today}_shops_data.json', 'w') as file:
            json.dump(data, file, indent=4)

        print("Datos descargados y guardados en la carpeta /datalake/shops_data")
    else:
        print(f"Error al realizar la solicitud: {response.status_code}")
