import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv
import pendulum
from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

load_dotenv()

BOGOTA_TZ = pendulum.timezone("America/Bogota")

DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")
FACTS_ROOT = DATA_LAKE_ROOT / "gold" / "facts"
FACT_EPISODES_PATH = FACTS_ROOT / "episodes.parquet"

EMAIL_SUBJECT = "Procesamiento de episodios finalizado"
EMAIL_BODY = """
El procesamiento del flujo de episodios ha finalizado exitosamente.

"""


def send_completion_email() -> None:
    """Envía el correo cuando el archivo de episodios está disponible."""
    email_sender = os.getenv("EMAIL_SENDER", "juandavid2931@gmail.com")
    email_password = os.getenv("EMAIL_PASSWORD", "ebin hvzu xfzw nqby")
    email_receiver = os.getenv("EMAIL_RECEIVER", "jdravila@bancolombia.com.co")

    message = EmailMessage()
    message["From"] = email_sender
    message["To"] = email_receiver
    message["Subject"] = EMAIL_SUBJECT
    message.set_content(EMAIL_BODY)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.send_message(message)


@dag(
    dag_id="fact_episodes_sensor",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "sensor"],
)
def fact_episodes_sensor_dag():
    wait_for_fact_episodes = FileSensor(
        task_id="wait_for_fact_episodes",
        filepath=str(FACT_EPISODES_PATH),
        poke_interval=5,
        timeout=3600,
        fs_conn_id="fs_default",
        mode="poke",
    )

    send_email_task = PythonOperator(
        task_id="notify_fact_episodes_ready",
        python_callable=send_completion_email,
    )

    wait_for_fact_episodes >> send_email_task


dag = fact_episodes_sensor_dag()