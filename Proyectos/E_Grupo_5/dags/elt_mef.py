import sys
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Añadir proyecto al path ---
PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- Importar funciones del ETL ---
from elt.ingest_raw_mef import ingest_to_raw
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver
from elt.fact_episodes import build_fact_episodes
from elt.dim_time import build_dim_time
from elt.dim_entidad import build_dim_entidad
from elt.dim_unidad import build_dim_unidad

# --- Zona horaria ---
LIMA_TZ = pendulum.timezone("America/Lima")

# --- Paths del Data Lake ---
DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

# Raw, Bronze, Silver
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "mef"
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "mef"
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "mef"

# Gold: hechos y dimensiones
FACT_PATH = DATA_LAKE_ROOT / "gold" / "facts" / "mef_data.parquet"
DIM_TIME_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "time.parquet"
DIM_ENTIDAD_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "entidad.parquet"
DIM_UNIDAD_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "unidad.parquet"

# --- Parámetros por capa ---
INGEST_PARAMS = {
    "output_dir": str(RAW_ROOT),
    "timeout": 30,
    "limit": 10,
}

BRONZE_PARAMS = {
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH / "mef.parquet"),
}

SILVER_PARAMS = {
    "bronze_path": str(BRONZE_PATH / "mef.parquet"),
    "silver_path": str(SILVER_PATH / "mef_silver.parquet"),
}

FACT_PARAMS = {
    "silver_path": str(SILVER_PATH / "mef_silver.parquet"),
    "output_path": str(FACT_PATH),
}

DIM_TIME_PARAMS = {
    "silver_path": str(SILVER_PATH / "mef_silver.parquet"),
    "output_path": str(DIM_TIME_PATH),
}

DIM_ENTIDAD_PARAMS = {
    "silver_path": str(SILVER_PATH / "mef_silver.parquet"),
    "output_path": str(DIM_ENTIDAD_PATH),
}

DIM_UNIDAD_PARAMS = {
    "silver_path": str(SILVER_PATH / "mef_silver.parquet"),
    "output_path": str(DIM_UNIDAD_PATH),
}

# --- Definición del DAG ---
with DAG(
    dag_id="elt_mef",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 10, 10, tz=LIMA_TZ),
    catchup=False,
    tags=["elt", "mef", "api"],
) as dag:

    # 1️⃣ Ingesta desde API
    ingest_task = PythonOperator(
        task_id="ingest_raw_mef",
        python_callable=ingest_to_raw,
        op_kwargs=INGEST_PARAMS,
    )

    # 2️⃣ Capa Bronze
    bronze_task = PythonOperator(
        task_id="copy_to_bronze",
        python_callable=copy_raw_to_bronze,
        op_kwargs=BRONZE_PARAMS,
    )

    # 3️⃣ Capa Silver
    silver_task = PythonOperator(
        task_id="to_silver",
        python_callable=transform_bronze_to_silver,
        op_kwargs=SILVER_PARAMS,
    )

    # 4️⃣ Dimensiones Gold
    time_task = PythonOperator(
        task_id="dim_time",
        python_callable=build_dim_time,
        op_kwargs=DIM_TIME_PARAMS,
    )

    entidad_task = PythonOperator(
        task_id="dim_entidad",
        python_callable=build_dim_entidad,
        op_kwargs=DIM_ENTIDAD_PARAMS,
    )

    unidad_task = PythonOperator(
        task_id="dim_unidad",
        python_callable=build_dim_unidad,
        op_kwargs=DIM_UNIDAD_PARAMS,
    )

    # 5️⃣ Hecho Gold
    fact_task = PythonOperator(
        task_id="fact_mef_data",
        python_callable=build_fact_episodes,
        op_kwargs=FACT_PARAMS,
    )

    # --- Dependencias ---
    ingest_task >> bronze_task >> silver_task
    silver_task >> [time_task, entidad_task, unidad_task] >> fact_task

