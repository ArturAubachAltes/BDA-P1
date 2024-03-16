import requests
import json

# URL de la API para realizar la consulta, asegúrate de incluir el formato de salida deseado, por ejemplo, 'f=json' para JSON
url = 'https://services6.arcgis.com/Do88DoK2xjTUCXd1/arcgis/rest/services/OSM_Shops_NA/FeatureServer/0/query?outFields=*&where=1%3D1&f=json'

# Realizar la solicitud a la API
response = requests.get(url)


# Verificar si la solicitud fue exitosa (código de estado 200)
if response.status_code == 200:
    # Convertir la respuesta en formato JSON a un diccionario de Python
    data = response.json()
    
    # Opcional: Guardar los datos en un archivo local
    with open('/datalake/datasearch/shops_data.json', 'w') as file:
        json.dump(data, file, indent=4)
    
    print("Datos descargados y guardados en 'shops_data.json'.")
else:
    print(f"Error al realizar la solicitud: {response.status_code}")
