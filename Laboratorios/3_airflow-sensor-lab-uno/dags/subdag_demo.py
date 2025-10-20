"""Very small DAG showing the deprecated SubDagOperator in the simplest form."""

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator

# Configuración básica compartida por el DAG padre y el subdag.
START_DATE = pendulum.datetime(2025, 10, 1, tz="America/Bogota")
DEFAULT_ARGS = {"start_date": START_DATE}

# 1) Definimos el subdag explícitamente con la API clásica.
subdag = DAG(
    dag_id="legacy_subdag_demo.deprecated_subdag",
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "subdag"],
)

with subdag:
    subtask_a = EmptyOperator(task_id="subtask_a")
    subtask_b = EmptyOperator(task_id="subtask_b")
    subtask_a >> subtask_b

# 2) Definimos el DAG principal e insertamos el SubDagOperator.
dag = DAG(
    dag_id="legacy_subdag_demo",
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "subdag"],
)

with dag:
    start = EmptyOperator(task_id="start")
    legacy_subdag = SubDagOperator(task_id="deprecated_subdag", subdag=subdag)
    end = EmptyOperator(task_id="end")

    start >> legacy_subdag >> end


