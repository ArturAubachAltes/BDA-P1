import requests
import os
from datetime import datetime, timedelta
import json



def descargar_dataworld(quin_dels_dos: str = "income"):  # income o sales
    if quin_dels_dos == "income":
        owner = 'jonloyens'
        dataset_id = 'irs-income-by-zip-code'
        carpeta = 'income_data'
        file_name = "IRSIncomeByZipCode_NoStateTotalsNoSmallZips.csv"

    elif quin_dels_dos == "sales":
        owner = 'ricjaramillo'
        dataset_id = 'sales'
        carpeta = 'sales_data'
        file_name = "SuperstoreSalesTraining.csv"

    else:
        raise ValueError("quin_dels_dos debe ser 'income' o 'cacatua'")

    token = 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OmFyYXVhbCIsImlzcyI6ImFnZW50OmFyYXVhbDo6NmVhMGRhNTMtNzU4Mi00ZGVhLTg3MDMtZjA3MjVlNzM0NzQzIiwiaWF0IjoxNzEwNDk4MDk4LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWUsInNhbWwiOnt9fQ.2tH0-oWkpqGRvccq5QYctJltMblavmk8rv6R_VUtODS_IqNz4qdchJcKla9jI5k7zGGeU3Q-nu8Rpry_hAnM9Q'

    url_metadata = f"https://api.data.world/v0/datasets/{owner}/{dataset_id}"

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }

    file_url = f"https://download.data.world/file_download/{owner}/{dataset_id}/{file_name}"

    response_file = requests.get(file_url, headers=headers, stream=True)

    if response_file.status_code == 200:
        today = datetime.now().strftime("%Y-%m-%d")
        data_path = f"./datalake/{carpeta}/{today}_{file_name}"

        directory = os.path.dirname(data_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(data_path, 'wb') as file:
            for chunk in response_file.iter_content(chunk_size=128):
                file.write(chunk)

        print(f"Archivo {file_name} descargado y guardado en {data_path}")
    else:
        print(f"Error al descargar el archivo {file_name}: {response_file.status_code}")


def descargar_datasearch():
    today = datetime.now().strftime("%Y-%m-%d")

    # URL de la API para realizar la consulta
    url = 'https://services6.arcgis.com/Do88DoK2xjTUCXd1/arcgis/rest/services/OSM_Shops_NA/FeatureServer/0/query?outFields=*&where=1%3D1&f=json'

    # Ahora compara la fecha más reciente con la fecha de hace 30 días
    response = requests.get(url)

    if response.status_code == 200:
        # Convertir la respuesta en formato JSON a un diccionario de Python
        data = response.json()

        with open(f'./datalake/shops_data/{today}_shops_data.json', 'w') as file:
            json.dump(data, file, indent=4)

        print("Datos descargados y guardados en la carpeta /datalake/shops_data")
    else:
        print(f"Error al realizar la solicitud: {response.status_code}")
