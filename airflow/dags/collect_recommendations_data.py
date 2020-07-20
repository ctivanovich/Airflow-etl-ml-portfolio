import numpy as np
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator

from ..modules.slack_alert import slack_alert, start_alert, end_alert

query_base = "./service-sql/recommendations_sql/"
uri_base = "gs://..../inputs/"

query_to_dest = {
    query_base + "common_users.sql" : uri_base + "users.csv",
    query_base + "implicit_v2_likes.sql": uri_base + "implicit-svd/likes_v2.csv",
    query_base + "images_v1_metadata.sql": uri_base + "image-clustering/image_metadata_ver_01.csv",
    query_base + "lightfm_v1_users.sql" : uri_base + "lightfm/users.csv",
    query_base + "lightfm_v1_likes.sql": uri_base + "lightfm/likes.csv"
}

dag = DAG(  
    dag_id="collect-recs-data",
    description="collecting recommendations data", 
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack_alert
        },
    schedule_interval="0 10 * * *", 
    start_date=datetime(2020, 2, 10),
    catchup=False
    )

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


tasks = [start, end]

for input_path, output_uri in query_to_dest.items():
    task_name = input_path.split('/')[-1][:-4]
    tasks.insert(
        -1,
        CloudSqlInstanceExportOperator(
            task_id = f"get_{task_name}",
            dag = dag,
            instance = '....', 
            body = {
                "exportContext": {
                    "fileType": "csv",
                    "uri": output_uri,
                    "csvExportOptions": {
                        "selectQuery": open(input_path, 'r').read()
                    }
                }
            },
            project_id='....', 
            gcp_conn_id='....', 
            api_version='v1beta4', 
            validate_body=True
            )
    )

for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]