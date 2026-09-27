from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
from sqlalchemy import create_engine

# ==========================================
# CONFIGURACIÓN DE INTERVALO EN SEGUNDOS
# ==========================================
EXECUTION_SECONDS = 10  # Modifica este valor en segundos

# Configuración por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_store_batch():
    """
    Extrae un fragmento del batch actual desde la Data API y lo almacena en PostgreSQL.
    """
    #api_url = "http://10.43.97.110:8080/data"
    
    print(f"Solicitando datos a: {api_url}")
    response = requests.get(
    "http://10.43.97.107:8025/data",
    params={"group_number": 3},
    timeout=60,
)
    response.raise_for_status()

    # Parsear los datos obtenidos de la API
    try:
        df = pd.DataFrame(response.json())
    except ValueError:
        df = pd.read_csv(io.StringIO(response.text), header=None)

    # Asignar los nombres correspondientes a las columnas del dataset
    column_names = [
        "Elevation", "Aspect", "Slope", "Horizontal_Distance_To_Hydrology",
        "Vertical_Distance_To_Hydrology", "Horizontal_Distance_To_Roadways",
        "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
        "Horizontal_Distance_To_Fire_Points", "Wilderness_Area", "Soil_Type", "Cover_Type"
    ]
    
    if len(df.columns) == len(column_names):
        df.columns = column_names
    else:
        print(f"Advertencia: El número de columnas recibidas ({len(df.columns)}) no coincide con las esperadas ({len(column_names)}).")

    # Conexión a PostgreSQL mediante SQLAlchemy utilizando el servicio interno 'postgres'
    db_url = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(db_url)
    
    # Insertar los datos en la tabla de forma incremental
    df.to_sql('forest_cover_data', engine, if_exists='append', index=False)
    print(f"✅ Se insertaron {len(df)} registros del batch actual en PostgreSQL.")

# Definición del DAG
with DAG(
    '1_dag_flujo',
    default_args=default_args,
    description='Extrae datos de la Data API por intervalos en segundos para los batches',
    schedule_interval=timedelta(seconds=EXECUTION_SECONDS),  # Intervalo expresado en segundos
    catchup=False,
    max_active_runs=1
) as dag:

    extract_load_task = PythonOperator(
        task_id='extract_and_load_batch',
        python_callable=fetch_and_store_batch
    )