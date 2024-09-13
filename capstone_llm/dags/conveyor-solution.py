from datetime import datetime, timedelta
from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2

role = "capstone_conveyor_llm"



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
    
    ConveyorSparkSubmitOperatorV2(
        task_id="clean_conv",
        mode="local",
        driver_instance_type="mx.medium",
        aws_role=role,
        application="src/capstonellm/tasks/clean.py",
        application_args=[
            "-e", "test",
        ]
    )
    