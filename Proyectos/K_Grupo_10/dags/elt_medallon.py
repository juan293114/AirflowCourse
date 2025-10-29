import sys
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Añadimos la ruta para que Airflow encuentre nuestros módulos
PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from dags.elt.bronze_layer import copy_raw_to_bronze_csv
from dags.elt.dim_region import build_dim_region
from dags.elt.dim_country import build_dim_country
from dags.elt.dim_language import build_dim_language
from dags.elt.dim_currency import build_dim_currency
from dags.elt.fact_country import build_fact_country

BOGOTA_TZ = pendulum.timezone("America/Bogota")

DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

# Capas
RAW_FILE = DATA_LAKE_ROOT / "raw" / "paises.csv"
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "paises_bronze.parquet"
GOLD_DIM_REGION = DATA_LAKE_ROOT / "gold" / "dim_region.parquet"
GOLD_DIM_COUNTRY = DATA_LAKE_ROOT / "gold" / "dim_country.parquet"
GOLD_DIM_LANGUAGE = DATA_LAKE_ROOT / "gold" / "dim_language.parquet"
GOLD_DIM_CURRENCY = DATA_LAKE_ROOT / "gold" / "dim_currency.parquet"
GOLD_FACT_COUNTRY = DATA_LAKE_ROOT / "gold" / "fact_country.parquet"

with DAG(
    dag_id="elt_paises",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 10, 26, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "paises"],
) as dag:

    # Copiar CSV a BRONZE
    bronze_task = PythonOperator(
        task_id="copy_to_bronze",
        python_callable=copy_raw_to_bronze_csv,
        op_kwargs={
            "raw_file": str(RAW_FILE),
            "bronze_path": str(BRONZE_PATH),
        },
    )

    # Dimensiones GOLD
    dim_region_task = PythonOperator(
        task_id="dim_region",
        python_callable=build_dim_region,
        op_kwargs={
            "bronze_path": str(BRONZE_PATH),
            "output_path": str(GOLD_DIM_REGION),
        },
    )

    dim_country_task = PythonOperator(
        task_id="dim_country",
        python_callable=build_dim_country,
        op_kwargs={
            "bronze_path": str(BRONZE_PATH),
            "output_path": str(GOLD_DIM_COUNTRY),
        },
    )

    dim_language_task = PythonOperator(
        task_id="dim_language",
        python_callable=build_dim_language,
        op_kwargs={
            "bronze_path": str(BRONZE_PATH),
            "output_path": str(GOLD_DIM_LANGUAGE),
        },
    )

    dim_currency_task = PythonOperator(
        task_id="dim_currency",
        python_callable=build_dim_currency,
        op_kwargs={
            "bronze_path": str(BRONZE_PATH),
            "output_path": str(GOLD_DIM_CURRENCY),
        },
    )

    # Fact Table
    fact_country_task = PythonOperator(
        task_id="fact_country",
        python_callable=build_fact_country,
        op_kwargs={
            "bronze_path": str(BRONZE_PATH),
            "output_path": str(GOLD_FACT_COUNTRY),
        },
    )

    # Flujo de tareas
    bronze_task >> [dim_region_task, dim_country_task, dim_language_task, dim_currency_task] >> fact_country_task
