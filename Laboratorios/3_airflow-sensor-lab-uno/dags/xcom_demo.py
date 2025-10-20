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
    def producer() -> str:
        value = "Hello from producer"
        print("Pushing value:", value)
        return value

    @task()
    def consumer(message: str) -> None:
        print("Pulled value via XCom:", message)

    consumer(producer())


dag = xcom_demo()
