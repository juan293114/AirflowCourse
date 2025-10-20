"""Simple example showing a main DAG triggering a downstream DAG (subDAG style)."""

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BOGOTA_TZ = pendulum.timezone("America/Bogota")


@dag(
    dag_id="child_subdag",
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["demo", "subdag"],
)
def child_subdag():
    EmptyOperator(task_id="child_task")


dag_child = child_subdag()


@dag(
    dag_id="parent_subdag",
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["demo", "subdag"],
)
def parent_subdag():
    start = EmptyOperator(task_id="start")

    run_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="child_subdag",
        wait_for_completion=True,
    )

    finish = EmptyOperator(task_id="finish")

    start >> run_child >> finish


dag_parent = parent_subdag()
