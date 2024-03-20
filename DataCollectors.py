from api_dataworld import *
from q import *

def datacollectors(income= False, dataserxh= False, sales= False, automatic= True):
    if automatic:
        for nom_carpeta, funcio, args in [("income_data", descargar_dataworld, "income"),
                                          ("shops_data", descargar_datastsrch, None),
                                          ("sales_data", descargar_dataworld, "sales")]:
            # Formato de fecha en los nombres de archivo
            date_format = "%Y-%m-%d"

            # Carpeta de búsqueda
            folder_path = f'./datalake/{nom_carpeta}'

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
                if args is not None:
                    funcio(args)
                else:
                    funcio()
                print(f"Database {nom_carpeta} actualitzada")
            else:
                print(f"Database {nom_carpeta} no actualitzada")

    else:
        if income == True:
            descargar_dataworld("income")
            print(f"Database income actualitzada")

            
        if dataserxh == True:
            descargar_datastsrch()
            print(f"Database dataserxh actualitzada")



        if sales == True:
            descargar_dataworld("sales")
            print(f"Database sales actualitzada")


datacollectors()
