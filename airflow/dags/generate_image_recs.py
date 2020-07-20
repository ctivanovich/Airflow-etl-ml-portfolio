import os

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator

from ..modules.slack_alert import slack_alert, start_alert, end_alert

IMAGE = "gcr.io/..../nbrun-image:1.0"

dag = DAG(  
    dag_id="generate-image-recs",
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        'on_failure_callback': slack_alert,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    },
    schedule_interval="0 11 * * *",
    start_date=datetime(2020, 2, 1), 
    catchup=False,
    )

host_vol = './production-ml-nbs/recommend-images-v1/'
container_vol = host_vol #just for readability, this is a 1 to 1 mapping from env to container

collect_nb = "obtain-images-prod-v1"
rec_nb = "images-recommend-v1"

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

start >> [
    DockerOperator(
        working_dir=container_vol,
        dag = dag,
        task_id = f"generate-recs-district-{district_id}",
        image = IMAGE, 
        command = f"papermill {rec_nb}.ipynb /dev/null -p DISTRICT_ID {district_id}", 
        cpus = 2.0, 
        auto_remove = True,
        docker_url='unix://var/run/docker.sock',
        network_mode= "bridge",
        tmp_dir='/tmp/airflow', 
        volumes=[host_vol + ":" + container_vol]
        ) for district_id in range(1,14)
] >> end

