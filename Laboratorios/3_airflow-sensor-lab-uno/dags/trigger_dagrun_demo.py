"""Demo: one DAG launches another using TriggerDagRunOperator."""

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BOGOTA_TZ = pendulum.timezone("America/Bogota")
DEFAULT_ARGS = {"start_date": pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ)}

CHILD_DAG_ID = "trigger_target_dag"
PARENT_DAG_ID = "trigger_launcher_dag"

# Child DAG: the DAG that will be triggered externally.
child_dag = DAG(
    dag_id=CHILD_DAG_ID,
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "trigger"],
)

with child_dag:
    child_start = EmptyOperator(task_id="child_start")
    child_task = EmptyOperator(task_id="child_task")
    child_end = EmptyOperator(task_id="child_end")

    child_start >> child_task >> child_end

# Parent DAG: triggers the child DAG when it runs.
parent_dag = DAG(
    dag_id=PARENT_DAG_ID,
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "trigger"],
)

with parent_dag:
    start = EmptyOperator(task_id="start")

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id=CHILD_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
    )

    finish = EmptyOperator(task_id="finish")

    start >> trigger_child >> finish


globals()[CHILD_DAG_ID] = child_dag
globals()[PARENT_DAG_ID] = parent_dag
