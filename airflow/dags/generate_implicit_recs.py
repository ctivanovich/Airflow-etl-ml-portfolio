import os

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator

from ..modules.slack_alert import slack_alert, start_alert, end_alert

dag = DAG(  
    dag_id="generate-implicit-recs",
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        'on_failure_callback': slack_alert,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    },
    schedule_interval="30 13 * * *",
    start_date=datetime(2020, 3, 11), 
    catchup=False,
    )

image = 'gcr.io/..../nbrun-cf:1.0'

host_vol = './production-ml-nbs/'
sep = ':'
container_vol = host_vol #just for readability, this is a 1 to 1 mapping from env to container

nb_path = "./production-ml-nbs/recommend-implicit-v2/"
nb_file = "implicit-v2.ipynb"

nb_tasks = []

start = PythonOperator(
    dag = dag,
    task_id = 'start',
    provide_context = True,
    python_callable = start_alert
    )

end = PythonOperator(
    dag = dag,
    task_id = 'end',
    provide_context = True,
    python_callable = end_alert
    )

for district in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]:
    nb_tasks.append(
        DockerOperator(
            working_dir = nb_path,
            dag = dag,
            task_id = f"generate-recs-district-{district}",
            image = image, 
            command = f"papermill {nb_file} {nb_file}_out.ipynb -p district_id {district}", 
            cpus = 1.0, 
            auto_remove = True,
            docker_url='unix://var/run/docker.sock',
            network_mode= "bridge",
            tmp_dir='/tmp/airflow', 
            volumes=[host_vol + sep + container_vol]
            )
        )

start >> nb_tasks >> end