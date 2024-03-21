from download_files import *
import re

datasets = {'income': descargar_dataworld,
            'shops' : descargar_datasearch,
            'sales' : descargar_dataworld}

def datacollectors(income= False, datasearch= False, sales= False):
    for nom, funcio in datasets.items():

        skip = False    

        if income == True:
            descargar_dataworld("income")
            print(f"Database income actualitzada")
            skip, income = True, False
            
        elif datasearch == True:
            descargar_datasearch()
            print(f"Database shops actualitzada")
            skip, datasearch = True, False

        elif sales == True:
            descargar_dataworld("sales")
            print(f"Database sales actualitzada")
            skip, sales = True, False

        if not skip:
            # Formato de fecha en los nombres de archivo
            date_format = "%Y-%m-%d"

            # Carpeta de búsqueda
            folder_path = f'./datalake/{nom}_data'

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
            
            if datetime.now() - timedelta(days=30) > latest_date:
                if nom != 'shops':
                    funcio(nom)
                else:
                    funcio()
                print(f"Database {nom} actualitzada")
            else:
                print(f"Database {nom} no actualitzada")




datacollectors(income=True, datasearch=True)
