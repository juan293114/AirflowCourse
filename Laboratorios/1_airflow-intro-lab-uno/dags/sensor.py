from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from pathlib import Path
from datetime import datetime, timedelta
import time

# Ruta del archivo
FILE_PATH = "/opt/airflow/raw/data_file.txt"

@dag(
    dag_id="simple_file_sensor_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule="@daily",
    catchup=False,
    tags=["lab", "sensor", "simple"]
)
def simple_file_sensor_flow():

    @task
    def generate_file():
        time.sleep(5)
        Path(FILE_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(FILE_PATH, "w") as f:
            f.write("Simulated data for processing.")
        print(f"File created at: {FILE_PATH}")

    @task
    def process_file():
        with open(FILE_PATH, "r") as f:
            content = f.read()
        print(f"Processing file with content: {content}")

    # Tarea 1: Crear archivo
    create = generate_file()

    # Tarea 2: Esperar archivo
    wait = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_PATH,
        poke_interval=10, # Intervalo de tiempo en segundos, en el que el sensor revisa
        timeout=60,
        mode="poke" # Modo poke (bloqueante síncrono) o reschedule (no bloqueante asíncrono)
    )

    # Tarea 3: Procesar archivo
    process = process_file()

    # Flujo de tareas
    create >> wait >> process

# Instancia del DAG
dag_instance = simple_file_sensor_flow()