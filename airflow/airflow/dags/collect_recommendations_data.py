import numpy as np
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

from slack_alert import slack_alert, start_alert, end_alert


USERS_QUERY = open("./recommendations-sql/users.sql").read()
USERS_URI = "gs://.../..../users.csv"

LIKES_QUERY = open("/./recommendations-sql/likes.sql").read()
LIKES_URI = "gs://.../inputs/.../likes.csv"

IMAGE_METADATA_QUERY = open("/.../recommendations-sql/image_metadata.sql").read()
IMAGE_METADATA_URI = "gs://.../inputs/.../image_metadata_ver_01.csv"

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
    schedule_interval="0 8 * * *", 
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

export_users = CloudSqlInstanceExportOperator(
    task_id = "get_users",
    dag = dag,
    instance = '...', 
    body = {
        "exportContext": {
            "fileType": "csv",
            "uri": USERS_URI,
            "csvExportOptions": {
                "selectQuery": USERS_QUERY
            }
        }
    },
    project_id='....', 
    gcp_conn_id='....', 
    api_version='v1beta4', 
    validate_body=True
)

export_likes = CloudSqlInstanceExportOperator(
        task_id = f"get_likes",
        dag = dag,
        instance = '...', 
        body = {
            "exportContext": {
                "fileType": "csv",
                "uri": LIKES_URI,
                "csvExportOptions": {
                    "selectQuery": LIKES_QUERY
                }
            }
        },
        project_id='...', 
        gcp_conn_id='...', 
        api_version='v1beta4', 
        validate_body=True
)

export_image_metadata = CloudSqlInstanceExportOperator(
        task_id = f"get_image_metadata",
        dag = dag,
        instance = '...', 
        body = {
            "exportContext": {
                "fileType": "csv",
                "uri": IMAGE_METADATA_URI,
                "csvExportOptions": {
                    "selectQuery": IMAGE_METADATA_QUERY
                }
            }
        },
        project_id='...', 
        gcp_conn_id='...', 
        api_version='v1beta4', 
        validate_body=True
)

start >> export_users >> export_likes >> export_image_metadata >> end