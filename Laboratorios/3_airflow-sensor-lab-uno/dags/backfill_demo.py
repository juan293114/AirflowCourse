"""Simple DAG to illustrate Airflow catchup/backfill behaviour."""

import pendulum
from airflow.decorators import dag, task

BOGOTA_TZ = pendulum.timezone("America/Bogota")


@dag(
    dag_id="backfill_demo",
    schedule="0 8 * * *",  # every day at 08:00
    start_date=pendulum.datetime(2025, 10, 16, tz=BOGOTA_TZ),
    catchup=True,
    tags=["demo", "catchup"],
)
def backfill_demo():
    @task()
    def log_context(**context) -> None:
        """Log timestamps to see each backfilled interval."""
        print("Context keys:", sorted(context.keys()))
        execution_date = context["logical_date"]
        print("Execution date:", execution_date.isoformat())

    log = log_context()
    log


dag = backfill_demo()
