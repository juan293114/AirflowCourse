"""Simple DAG to illustrate TaskGroup usage."""
import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

BOGOTA_TZ = pendulum.timezone("America/Bogota")


@dag(
    dag_id="taskgroup_demo",
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["demo", "taskgroup"],
)
def taskgroup_demo():
    @task()
    def start() -> None:
        print("Starting DAG")

    with TaskGroup(group_id="example_group") as example_group:
        @task(task_id="task_a")
        def task_a() -> None:
            print("Task A inside TaskGroup")

        @task(task_id="task_b")
        def task_b() -> None:
            print("Task B inside TaskGroup")

        task_a() >> task_b()

    @task()
    def end() -> None:
        print("Finishing DAG")

    start() >> example_group >> end()


dag = taskgroup_demo()
