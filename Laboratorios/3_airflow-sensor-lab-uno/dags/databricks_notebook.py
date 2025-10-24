from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

DATABRICKS_CONN_ID = "databricks_default"
EXISTING_CLUSTER_ID = "1017-023830-ncttgxay"

@dag(
    dag_id="databricks_notebook_task_dag",
    start_date=datetime(2025, 10, 23),
    schedule=None,
    catchup=False,
    tags=["databricks", "notebook"]
)
def databricks_job_dag():

    # 1) Tarea de ingesta a RAW
    ingest_to_raw = DatabricksSubmitRunOperator(
        task_id="ingest_to_raw",
        databricks_conn_id=DATABRICKS_CONN_ID,
        # Si quieres capturar run_id y salida del notebook en XCom:
        do_xcom_push=True,
        json={
            "existing_cluster_id": EXISTING_CLUSTER_ID,
            "notebook_task": {
                "notebook_path": "/Users/juramireza@unal.edu.co/4_airflow-databricks-lab-tres/elt/1_Ingest_to_raw",
                "base_parameters": {
                    "start_date": "2024-12-01",
                    "end_date": "2024-12-31",
                    "output_dir": "/dbfs/FileStore/datalake/raw",
                    "timeout": "30"
                }
            },
            # Opcional: tiempo máx. a nivel de run
            "timeout_seconds": 60 * 60,  # 1 hora
        },
    )

    # 2) Tarea de transformación a BRONZE que depende de la 1)
    transform_to_bronze = DatabricksSubmitRunOperator(
        task_id="copy_to_bronze",
        databricks_conn_id=DATABRICKS_CONN_ID,
        do_xcom_push=True,
        json={
            "existing_cluster_id": EXISTING_CLUSTER_ID,
            "notebook_task": {
                "notebook_path": "/Users/juramireza@unal.edu.co/4_airflow-databricks-lab-tres/elt/2_bronze",
                "base_parameters": {
                    # Podrías reutilizar los mismos parámetros o derivarlos
                    "raw_root": "/dbfs/FileStore/datalake/raw",
                    "bronze_path": "dbfs:/FileStore/datalake/bronze/tvmaze",
                }
            },
            "timeout_seconds": 60 * 60,
        },
    )

    # 2) Tarea de transformación a SILVER que depende de la 2)
    transform_to_silver = DatabricksSubmitRunOperator(
        task_id="transform_to_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        do_xcom_push=True,
        json={
            "existing_cluster_id": EXISTING_CLUSTER_ID,
            "notebook_task": {
                "notebook_path": "/Users/juramireza@unal.edu.co/4_airflow-databricks-lab-tres/elt/3_silver",
                "base_parameters": {
                    # Podrías reutilizar los mismos parámetros o derivarlos
                    "bronze_path": "dbfs:/FileStore/datalake/bronze/tvmaze",
                    "silver_path": "dbfs:/FileStore/datalake/silver/tvmaze",
                }
            },
            "timeout_seconds": 60 * 60,
        },
    )

    # Encadenamiento: primero ingesta, luego transformación
    ingest_to_raw >> transform_to_bronze >> transform_to_silver

databricks_job_dag()
