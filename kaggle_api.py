import os
from kaggle.api.kaggle_api_extended import KaggleApi

#Tinc una carpeta creada a  C:\Users\paula\.kaggle amb la meva API
#Si no, no es pot agafar les dades
#cal fer un pip install kaggle abans


# Inicializa la API
api = KaggleApi()
api.authenticate()

# URL del dataset en Kaggle
dataset_url = 'thedevastator/2013-irs-us-income-data-by-zip-code'
dataset_file_name = 'IRSIncomeByZipCode.csv'

# Descargar el conjunto de datos específico
api.dataset_download_file(dataset_url, dataset_file_name, path='./')

# Si el archivo se descarga como zip, puedes descomprimirlo así
if os.path.exists(f"{dataset_file_name}.zip"):
    import zipfile
    with zipfile.ZipFile(f"{dataset_file_name}.zip", 'r') as zip_ref:
        zip_ref.extractall("./")
    os.remove(f"{dataset_file_name}.zip")  # Opcional: eliminar el archivo zip después de descomprimir
