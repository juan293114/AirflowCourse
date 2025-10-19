"""Simple DAG to illustrate Airflow catchup/backfill behaviour."""

import pendulum
from airflow.decorators import dag, task

BOGOTA_TZ = pendulum.timezone("America/Bogota")


@dag(
    dag_id="backfill_demo",
    schedule="0 8 * * *",  # every day at 08:00
    start_date=pendulum.datetime(2025, 10, 15, tz=BOGOTA_TZ),
    catchup=True,
    tags=["demo", "catchup"],
)
def backfill_demo():
    @task()
    def log_context(execution_date: pendulum.DateTime, data_interval_end: pendulum.DateTime) -> None:
        """Log timestamps so you can see each backfilled interval."""
        print("Execution date:", execution_date.isoformat())
        print("Data interval end:", data_interval_end.isoformat())

    log_context()


dag = backfill_demo()
