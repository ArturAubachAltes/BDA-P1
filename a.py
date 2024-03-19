import requests
import os
import json
from datetime import datetime



def descargar_dataworld(quin_dels_dos:str= "income"): #income o sales
    if quin_dels_dos == "income":
        owner = 'jonloyens'
        dataset_id = 'irs-income-by-zip-code'
        carpeta = 'income_data'


    elif quin_dels_dos == "sales":  
        owner = 'ricjaramill    o'
        dataset_id = 'sales'
        carpeta = 'sales_data'

    else:
        raise ValueError("quin_dels_dos debe ser 'income' o 'cacatua'")

    token = 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OmFyYXVhbCIsImlzcyI6ImFnZW50OmFyYXVhbDo6NmVhMGRhNTMtNzU4Mi00ZGVhLTg3MDMtZjA3MjVlNzM0NzQzIiwiaWF0IjoxNzEwNDk4MDk4LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWUsInNhbWwiOnt9fQ.2tH0-oWkpqGRvccq5QYctJltMblavmk8rv6R_VUtODS_IqNz4qdchJcKla9jI5k7zGGeU3Q-nu8Rpry_hAnM9Q'

    url = f"https://api.data.world/v0/datasets/{owner}/{dataset_id}"

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }

   
    response_metadata = requests.get(url, headers=headers)
    if response_metadata.status_code == 200:
        files = response_metadata.json().get('files', [])
        for file_info in files:
            file_name = file_info['name']
            file_url = f"https://api.data.world/v0/file_download/{owner}/{dataset_id}/{file_name}"

            # Realizar la petición para descargar el archivo
            response_file = requests.get(file_url, headers=headers, stream=True)
            if response_file.status_code == 200:
                today = datetime.now().strftime("%Y-%m-%d")
                data_path = f"./datalake/{carpeta}/{today}_{file_name}"

                # Crear el directorio si no existe
                directory = os.path.dirname(data_path)
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Guardar el archivo descargado
                with open(data_path, 'wb') as file:
                    for chunk in response_file.iter_content(chunk_size=128):
                        file.write(chunk)

                print(f"Archivo {file_name} descargado y guardado en {data_path}")
            else:
                print(f"Error al descargar el archivo {file_name}: {response_file.status_code}")
    else:
        print(f"Error al obtener metadatos: {response_metadata.status_code}")

# Ejemplo de uso:
descargar_dataworld("sales")