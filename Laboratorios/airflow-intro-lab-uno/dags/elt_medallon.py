import sys
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from elt.ingest_raw import ingest_to_raw

BOGOTA_TZ = pendulum.timezone("America/Bogota")

DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "tvmaze"

INGEST_PARAMS = {
    "start_date": pendulum.date(2020, 1, 1),
    "end_date": pendulum.date(2020, 1, 31),
    "output_dir": str(RAW_ROOT),
    "timeout": 30,
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
    
    ingest_task 