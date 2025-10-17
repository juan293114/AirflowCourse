import sys
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from elt.ingest_raw import ingest_to_raw
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver
from elt.fact_episodes import build_fact_episodes
from elt.dim_time import build_dim_time
from elt.dim_shows import build_dim_shows 
from elt.dim_networks import build_dim_networks

BOGOTA_TZ = pendulum.timezone("America/Bogota")


DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

#Capa raw
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "tvmaze"

#Capa bronze
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "tvmaze"

#Capa Silver 
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "tvmaze"

#Fact episdodes
FACT_EPISODES_PATH = DATA_LAKE_ROOT / "gold" / "facts" / "episodes.parquet"

#Dim Time
DIM_TIME_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "time.parquet"
#Dim Show
DIM_SHOW_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "show.parquet"
#Dim Network
DIM_NETWORK_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "network.parquet"

INGEST_PARAMS = {
    "start_date": pendulum.date(2020, 1, 1),
    "end_date": pendulum.date(2020, 1, 31),
    "output_dir": str(RAW_ROOT),
    "timeout": 30,
}

BRONZE_PARAMS = { 
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH / "tvmaze.parquet"),
}

SILVER_PARAMS ={
    "bronze_path": str(BRONZE_PATH / "tvmaze.parquet"),
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
}

EPISODES_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(FACT_EPISODES_PATH),
}

DIM_TIME_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_TIME_PATH),
}

DIM_SHOW_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_SHOW_PATH),
}

DIM_NETWORK_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_NETWORK_PATH),
}


with DAG(

    dag_id="elt_medallon",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 10, 10, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "api"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_to_raw,
        op_kwargs=INGEST_PARAMS,
    )

    bronze_task = PythonOperator(
        task_id="copy_to_bronze",
        python_callable=copy_raw_to_bronze,
        op_kwargs=BRONZE_PARAMS,
    )
    silver_task = PythonOperator(
        task_id="to_silver",
        python_callable=transform_bronze_to_silver,
        op_kwargs=SILVER_PARAMS,
    )
    time_task = PythonOperator(
        task_id="dim_time",
        python_callable=build_dim_time,
        op_kwargs=DIM_TIME_PARAMS,
    )
    show_task = PythonOperator(
        task_id="dim_show",
        python_callable=build_dim_shows,
        op_kwargs=DIM_SHOW_PARAMS,
    )
    network_task = PythonOperator(
        task_id="dim_network",
        python_callable=build_dim_networks,
        op_kwargs=DIM_NETWORK_PARAMS,
    )
    episodes_task = PythonOperator(
        task_id="fact_episodes",
        python_callable=build_fact_episodes,
        op_kwargs=EPISODES_PARAMS,
    )

    ingest_task >> bronze_task >> silver_task >> [time_task, show_task, network_task] >> episodes_task
