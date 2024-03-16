import requests
import zipfile
import os
from datetime import datetime

# Sustituye 'your_token' con el token de acceso que copiaste
token = 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OnBhdWxhZ2FsdnZleiIsImlzcyI6ImFnZW50OnBhdWxhZ2FsdnZlejo6YTczNTIzYzMtMjM2ZC00MjU1LWEwMDYtOWEwNzU1ZWZkNmJiIiwiaWF0IjoxNzEwNDQxMDY4LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWUsInNhbWwiOnt9fQ.mR3oquR4uMYCqRVgWRIhjn4Zpd1UfziXC8jDn8gRZh9CH1Q-FCbGGY5ldKZR0LSqGWqYLmtt60E0JG9yn7YdUA'


# Estos son el dueño y el identificador del conjunto de datos en data.world
owner = 'ricjaramillo'
dataset_id = 'sales'

url = f'https://api.data.world/v0/download/{owner}/{dataset_id}'

headers = {
    'Authorization': f'Bearer {token}',
    'Accept': 'application/zip'
}


# Realiza la petición para descargar el conjunto de datos
response = requests.get(url, headers=headers)

# Comprueba que la petición fue exitosa
if response.status_code == 200:
    # Define el nombre del archivo temporal zip
    zip_filename = f'{dataset_id}.zip'
    
    # Escribe el contenido en el archivo zip
    with open(zip_filename, 'wb') as f:
        f.write(response.content)
    
    # Extrae el contenido del archivo zip
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall('datalake/data_world')
    
    # Obtiene la fecha de hoy
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Renombra los archivos extraídos añadiendo la fecha actual al final de cada nombre de archivo
    for root, dirs, files in os.walk('datalake/data_world'):
        for file in files:
            # Define el nuevo nombre del archivo con la fecha al final
            new_filename = f"{file.rsplit('.', 1)[0]}_{today}.{file.rsplit('.', 1)[1]}"
            os.rename(os.path.join(root, file), os.path.join(root, new_filename))
    
    # Elimina el archivo zip ya que no se necesita más
    os.remove(zip_filename)
    
    print(f"Los archivos han sido descargados y descomprimidos en 'datalake/data_world' con la fecha añadida.")
else:
    print("Error en la descarga:", response.status_code)