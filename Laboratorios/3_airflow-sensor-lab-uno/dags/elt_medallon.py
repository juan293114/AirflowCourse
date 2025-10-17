import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

# Ajuste del path para importar los módulos
PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Importación de funciones de ETL
from elt.ingest_raw import ingest_to_raw
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver
from elt.dim_shows import build_dim_shows
from elt.dim_networks import build_dim_networks
from elt.dim_time import build_dim_time
from elt.fact_episodes import build_fact_episodes

# Configuración de constantes y rutas
BOGOTA_TZ = pendulum.timezone("America/Bogota")

DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "tvmaze"
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "tvmaze" / "tvmaze.parquet"
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "tvmaze" / "tvmaze.parquet"
DIMENSIONS_ROOT = DATA_LAKE_ROOT / "gold" / "dimensions"
FACTS_ROOT = DATA_LAKE_ROOT / "gold" / "facts"

DIM_SHOWS_PATH = DIMENSIONS_ROOT / "shows.parquet"
DIM_NETWORKS_PATH = DIMENSIONS_ROOT / "networks.parquet"
DIM_DATES_PATH = DIMENSIONS_ROOT / "dates.parquet"
FACT_EPISODES_PATH = FACTS_ROOT / "episodes.parquet"

INGEST_PARAMS = {
    "start_date": pendulum.date(2024, 1, 1),
    "end_date": pendulum.date(2024, 1, 31),
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
DIM_SHOWS_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(DIM_SHOWS_PATH),
}
DIM_NETWORKS_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(DIM_NETWORKS_PATH),
}
DIM_DATES_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(DIM_DATES_PATH),
}
FACT_EPISODES_PARAMS = {
    "silver_path": str(SILVER_PATH),
    "output_path": str(FACT_EPISODES_PATH),
}


# -----------------------------------
# DAG con decoradores
# -----------------------------------

@dag(
    dag_id="elt_medallon",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "api"],
)
def elt_medallon_dag():
    @task()
    def ingest_raw():
        ingest_to_raw(**INGEST_PARAMS)

    @task()
    def copy_to_bronze():
        copy_raw_to_bronze(**BRONZE_PARAMS)

    @task()
    def to_silver():
        transform_bronze_to_silver(**SILVER_PARAMS)

    @task()
    def dim_show():
        build_dim_shows(**DIM_SHOWS_PARAMS)

    @task()
    def dim_network():
        build_dim_networks(**DIM_NETWORKS_PARAMS)

    @task()
    def dim_time():
        build_dim_time(**DIM_DATES_PARAMS)

    @task()
    def fact_episodes():
        build_fact_episodes(**FACT_EPISODES_PARAMS)

    # Orquestación
    i = ingest_raw()
    b = copy_to_bronze()
    s = to_silver()
    d_shows = dim_show()
    d_networks = dim_network()
    d_dates = dim_time()
    f_episodes = fact_episodes()

    i >> b >> s >> [d_shows, d_networks, d_dates] >> f_episodes


# Instanciamos el DAG
dag = elt_medallon_dag()
