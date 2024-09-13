from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "cleaning_dag",
    default_args=default_args,
    schedule_interval=" * * 1 * *",
    catchup=False,
) as dag:
    clean = DockerOperator(
        task_id="clean",
        image="my-app",
        command="python3 -m capstonellm.tasks.clean -e local",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        network_mode="bridge",
        environment= {
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'AWS_SESSION_TOKEN': os.getenv('AWS_SESSION_TOKEN')
        }
    )


    