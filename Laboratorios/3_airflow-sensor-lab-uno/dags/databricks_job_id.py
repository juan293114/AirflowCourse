from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

# Definición del DAG con decorador
@dag(
    dag_id="databricks_job_example",
    start_date=datetime(2025, 10, 23),
    schedule="@daily",
    catchup=False,
    tags=["databricks", "example"]
)
def databricks_job_dag():

    # Tarea para ejecutar el job en Databricks
    DatabricksRunNowOperator(
        task_id="elt_medallon_job",
        databricks_conn_id="databricks_default",  # ID de conexión configurado en Airflow
        job_id=236093442951910,  # ID del job en Databricks
    )

# Instancia del DAG
databricks_job_dag()