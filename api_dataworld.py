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

    response = requests.get(url, headers=headers)

    # Realiza la petición para descargar el conjunto de datos
    today = datetime.now().strftime("%Y-%m-%d")
    data_path = f'./datalake/{carpeta}/{today}_{carpeta}.json'

    # Crear el directorio si no existe
    directory = os.path.dirname(data_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Verificar si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        data = response.json()
        with open(data_path, 'w') as file:
            json.dump(data, file, indent=4)
        
        print(f"Datos descargados y guardados en la carpeta /{carpeta}/data_world")
    else:
        print(f"Error al realizar la solicitud: {response.status_code}")


descargar_dataworld("income")