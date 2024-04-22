import os
import duckdb
import pandas as pd

# Lista de conjuntos de datos a procesar
datasets = ["income_data", "sales_data", "shops_data"]
base_path = "datalake"

# Crear una conexión a DuckDB
con = duckdb.connect(database='./database.duckdb', read_only=False)

# Función para cargar datos, actualizar y almacenar
def update_tables(dataset_name):
    # Construir el path del directorio
    directory_path = os.path.join(base_path, dataset_name)
    
    # Obtener el primer archivo .parquet en el directorio
    files = [file for file in os.listdir(directory_path) if file.endswith('.parquet')]
    if not files:
        print(f"No se encontraron archivos .parquet en {directory_path}")
        return
    first_parquet_file = files[0]
    file_path = os.path.join(directory_path, first_parquet_file)
    
    # Leer el archivo parquet con pandas
    df = pd.read_parquet(file_path)
    
    # Registrar el DataFrame como una vista en DuckDB
    con.register(f"{dataset_name}_view", df)
    
    # Verificar si la tabla existe
    exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{dataset_name}'").fetchone()[0] > 0
    
    if not exists:
        # Crear la tabla si no existe
        con.execute(f"CREATE TABLE {dataset_name} AS SELECT * FROM {dataset_name}_view")
    else:
        # Actualizar la tabla si ya existe
        con.execute(f"""
        INSERT INTO {dataset_name}
        SELECT * FROM {dataset_name}_view
        WHERE NOT EXISTS (SELECT 1 FROM {dataset_name} WHERE {dataset_name}.id = {dataset_name}_view.id)
        """)
    
    # Desregistrar la vista para limpiar
    con.unregister(f"{dataset_name}_view")

    # Verificar si existe la carpeta Formatted_Zone y guardar los cambios
    output_folder = 'Formatted_Zone'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = os.path.join(output_folder, f"{dataset_name}.parquet")
    con.execute(f"COPY {dataset_name} TO '{output_file}' (FORMAT 'parquet')")

# Procesar cada dataset según la elección del usuario
for dataset in datasets:  # Aquí puedes implementar una selección basada en inputs del usuario
    update_tables(dataset)
