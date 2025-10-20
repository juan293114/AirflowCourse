"""Simple DAG to illustrate trigger rules."""

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

BOGOTA_TZ = pendulum.timezone("America/Bogota")


@dag(
    dag_id="triggerrule_demo",
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["demo", "trigger_rule"],
)
def triggerrule_demo():
    start = EmptyOperator(task_id="start")

    success_task = EmptyOperator(task_id="always_success")

    @task()
    def fail() -> None:
        raise ValueError("Intentional failure to show trigger rule")

    failure_task = fail()

    join_any = EmptyOperator(
        task_id="wait_for_any",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    end = EmptyOperator(task_id="end")

    start >> [success_task, failure_task]
    success_task >> join_any
    failure_task >> join_any
    join_any >> end


dag = triggerrule_demo()
