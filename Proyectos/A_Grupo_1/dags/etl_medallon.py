import os
import sys

from pathlib import Path

import pendulum
from airflow.decorators import dag, task


# --- Ajuste del path para importar los módulos ---
PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- Importación de funciones de ETL (TU PROYECTO) ---
from elt.ingest_raw import ingest_to_raw
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver
from elt.dim_aeronave import build_dim_aeronave
from elt.dim_fuente_posicion import build_dim_fuente_posicion
from elt.dim_tiempo_snapshot import build_dim_tiempo_snapshot
from elt.fact_trafico_aereo import build_hechos_trafico_aereo

# --- Configuración de constantes y rutas ---
LIMA_TZ = pendulum.timezone("America/Lima")

DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "opensky_states"
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "opensky_states" / "opensky_states.parquet"
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "opensky_states" / "opensky_states_silver.parquet"

DIMENSIONS_ROOT = DATA_LAKE_ROOT / "gold" / "dimensions"
FACTS_ROOT = DATA_LAKE_ROOT / "gold" / "facts"

# Paths de Capa Gold
DIM_AERONAVE_PATH = DIMENSIONS_ROOT / "dim_aeronave.parquet"
DIM_FUENTE_PATH = DIMENSIONS_ROOT / "dim_fuente_posicion.parquet"
DIM_TIEMPO_PATH = DIMENSIONS_ROOT / "dim_tiempo_snapshot.parquet"
FACT_TRAFICO_PATH = FACTS_ROOT / "hechos_trafico_aereo.parquet"

# --- Definición de Parámetros para las Tareas ---

INGEST_PARAMS = {
    "output_dir": str(RAW_ROOT),
    "timeout": 30,
}
BRONZE_PARAMS = {
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH),
}
SILVER_PARAMS = {
    "bronze_path": str(BRONZE_PATH),
    "silver_path": str(SILVER_PATH),
}
DIM_AERONAVE_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(DIM_AERONAVE_PATH),
}
DIM_FUENTE_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(DIM_FUENTE_PATH),
}
DIM_TIEMPO_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(DIM_TIEMPO_PATH),
}
HECHOS_TRAFICO_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(FACT_TRAFICO_PATH),
}



# -----------------------------------
# DAGs
# -----------------------------------

@dag(
    dag_id="elt_medallon_opensky_states",
    schedule="0 * * * *", # Cada hora
    start_date=pendulum.datetime(2025, 10, 10, tz=LIMA_TZ),
    catchup=False,
    tags=["elt", "api", "opensky", "medallion"],
)
def elt_medallon_opensky_states_dag():
    """
    DAG que ingesta datos en vivo de OpenSky y los procesa a través de la
    arquitectura Medallón (Bronce, Silver, Gold).
    """
    
    # --- Tareas de Capas Base ---
    
    @task()
    def ingest_raw():
        return ingest_to_raw(**INGEST_PARAMS)

    @task()
    def copy_to_bronze():
        return copy_raw_to_bronze(**BRONZE_PARAMS)

    @task()
    def to_silver():
        return transform_bronze_to_silver(**SILVER_PARAMS)

    # --- Tareas de Capa Gold (Dimensiones) ---
    
    @task()
    def dim_aeronave():
        return build_dim_aeronave(**DIM_AERONAVE_PARAMS)

    @task()
    def dim_fuente_posicion():
        return build_dim_fuente_posicion(**DIM_FUENTE_PARAMS)

    @task()
    def dim_tiempo_snapshot():
        return build_dim_tiempo_snapshot(**DIM_TIEMPO_PARAMS)

    # --- Tarea de Capa Gold (Hechos) ---
    
    @task()
    def hechos_trafico_aereo():
        return build_hechos_trafico_aereo(**HECHOS_TRAFICO_PARAMS)

    

    # --- Orquestación ---
    i = ingest_raw()
    b = copy_to_bronze()
    s = to_silver()
    
    d_aeronave = dim_aeronave()
    d_fuente = dim_fuente_posicion()
    d_tiempo = dim_tiempo_snapshot()
    
    f_trafico = hechos_trafico_aereo()
    
    i >> b >> s
    s >> [d_aeronave, d_fuente, d_tiempo]
    [d_aeronave, d_fuente, d_tiempo] >> f_trafico
    


# Instanciamos el DAG
dag = elt_medallon_opensky_states_dag()