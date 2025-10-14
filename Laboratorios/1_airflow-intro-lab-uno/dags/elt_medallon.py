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
from elt.fact_episodes import create_fact_episodes

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
    episodes_task = PythonOperator(
        task_id="fact_episodes",
        python_callable=create_fact_episodes,
        op_kwargs=EPISODES_PARAMS,
    )

    ingest_task >> bronze_task >> silver_task >> episodes_task
