"""Minimal DAG to demonstrate XCom push/pull."""

import pendulum
from airflow.decorators import dag, task

BOGOTA_TZ = pendulum.timezone("America/Bogota")


@dag(
    dag_id="xcom_demo",
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["demo", "xcom"],
)
def xcom_demo():
    @task()
    def producer(**context) -> None:
        value = "Hello from producer"
        print("Pushing value via xcom_push:", value)
        context["ti"].xcom_push(key="greeting", value=value)

    @task()
    def consumer(**context) -> None:
        message = context["ti"].xcom_pull(task_ids="producer", key="greeting")
        print("Pulled value via xcom_pull:", message)

    producer_task = producer()
    consumer_task = consumer()
    producer_task >> consumer_task


dag = xcom_demo()
