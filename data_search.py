import os, re, json
import requests
from datetime import datetime, timedelta


# Formato de fecha en los nombres de archivo
date_format = "%Y-%m-%d"

# Carpeta de búsqueda
folder_path = './datalake/shops_data'

# Inicializar variables para guardar el nombre del archivo más reciente y su fecha
latest_date = datetime.min
latest_file_name = ''
today = datetime.now().strftime("%Y-%m-%d")

# Primera pasada para encontrar el archivo más reciente
for filename in os.listdir(folder_path):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        file_date = datetime.strptime(match.group(), date_format)
        if file_date > latest_date:
            latest_date = file_date
            latest_file_name = filename

# Segunda pasada para eliminar todos los archivos excepto el más reciente
for filename in os.listdir(folder_path):
    if filename != latest_file_name:
        os.remove(os.path.join(folder_path, filename))  

# URL de la API para realizar la consulta
url = 'https://services6.arcgis.com/Do88DoK2xjTUCXd1/arcgis/rest/services/OSM_Shops_NA/FeatureServer/0/query?outFields=*&where=1%3D1&f=json'


# Ahora compara la fecha más reciente con la fecha de hace 30 días
if datetime.now() - timedelta(days=30) > latest_date:
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
