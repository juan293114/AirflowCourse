import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BOGOTA_TZ = pendulum.timezone("America/Bogota")
DEFAULT_ARGS = {"start_date": pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ)}

# Child DAG: the DAG that will be triggered externally.
trigger_target_dag = DAG(
    dag_id="trigger_target_dag",
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "trigger"],
)

with trigger_target_dag:
    child_start = EmptyOperator(task_id="child_start")
    child_task = EmptyOperator(task_id="child_task")
    child_end = EmptyOperator(task_id="child_end")

    child_start >> child_task >> child_end

# Parent DAG: triggers the child DAG when it runs.
trigger_launcher_dag = DAG(
    dag_id="trigger_launcher_dag",
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "trigger"],
)

with trigger_launcher_dag:
    start = EmptyOperator(task_id="start")

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="trigger_target_dag",
        wait_for_completion=True,
        poke_interval=10,
    )

    finish = EmptyOperator(task_id="finish")

    start >> trigger_child >> finish