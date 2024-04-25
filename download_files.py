import requests
import os
from datetime import datetime, timedelta
import json
import pandas as pd


import requests
import os
from datetime import datetime
import pandas as pd


# income o sales
def descargar_dataworld(quin_dels_dos: str = "income"):  # income o sales
    if quin_dels_dos == "income":
        owner = 'jonloyens'
        dataset_id = 'irs-income-by-zip-code'
        carpeta = 'income_data'
        file_name = "IRSIncomeByZipCode_NoStateTotalsNoSmallZips.csv"
        output_file_name = "IRSIncomeByZipCode_NoStateTotalsNoSmallZips.parquet"

    elif quin_dels_dos == "sales":
        owner = 'ricjaramillo'
        dataset_id = 'sales'
        carpeta = 'sales_data'
        file_name = "SuperstoreSalesTraining.csv"
        output_file_name = "SuperstoreSalesTraining.parquet"

    else:
        raise ValueError("quin_dels_dos debe ser 'income' o 'sales'")

    token = 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OmFkc3NkYWRhc2RhcyIsImlzcyI6ImFnZW50OmFkc3NkYWRhc2Rhczo6NWJkYTI3YTctZGEyMi00OWQxLTg0NzYtMTAwZTIzM2Q5ODViIiwiaWF0IjoxNzE0MDY3OTIwLCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWUsInNhbWwiOnt9fQ.wfslA5c1ESO7-E8VSmdEr0wMQbG-800XgM9QYIdjVAwhr072lV7Li2HxeWR5rh1PTqkdfOh4qNyqFa5IXrAkaA'

    file_url = f"https://download.data.world/file_download/{owner}/{dataset_id}/{file_name}"

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }

    response_file = requests.get(file_url, headers=headers, stream=True)

    if response_file.status_code == 200:
        today = datetime.now().strftime("%Y-%m-%d")
        csv_path = f"./datalake/{carpeta}/{today}_{file_name}"
        parquet_path = f"./datalake/{carpeta}/{today}_{output_file_name}"

        directory = os.path.dirname(csv_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(csv_path, 'wb') as file:
            for chunk in response_file.iter_content(chunk_size=128):
                file.write(chunk)

        
        # Cargar CSV y guardar como Parquet

        df = pd.read_csv(csv_path, encoding='ISO-8859-1')
        df.to_parquet(parquet_path, index=False)

        # Opcional: Eliminar el archivo CSV después de la conversión
        os.remove(csv_path)

        print(f"Archivo {output_file_name} descargado y convertido a Parquet y guardado en {parquet_path}")
    else:
        print(f"Error al descargar el archivo {file_name}: {response_file.status_code}")


def descargar_datasearch():
    today = datetime.now().strftime("%Y-%m-%d")
    url = 'https://services6.arcgis.com/Do88DoK2xjTUCXd1/arcgis/rest/services/OSM_Shops_NA/FeatureServer/0/query?outFields=*&where=1%3D1&f=json'
    response = requests.get(url)

    if response.status_code == 200:
        # Suponiendo que los datos están en 'features'
        data = response.json()['features']
        df = pd.json_normalize(data)
        data_path = f'./datalake/shops_data/{today}_shops_data.parquet'
        directory = os.path.dirname(data_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_parquet(data_path)
        print(f"Datos descargados y guardados como Parquet en {data_path}")
    else:
        print(f"Error al realizar la solicitud: {response.status_code}")
