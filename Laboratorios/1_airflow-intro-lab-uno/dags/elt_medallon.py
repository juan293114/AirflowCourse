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

BOGOTA_TZ = pendulum.timezone("America/Bogota")


DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

#Capa raw
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "tvmaze"

#Capa bronze
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "tvmaze"

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
    
    ingest_task >> bronze_task 