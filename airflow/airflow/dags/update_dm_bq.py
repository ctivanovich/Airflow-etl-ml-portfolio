import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

from slack_alert import slack_alert, start_alert, end_alert

dag = DAG(  
    dag_id="update-mj-dm-bq",
    description="Importation of masked MJ data into Dart Mart", 
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': slack_alert
        },
    schedule_interval="30 8 * * *", 
    start_date=datetime(2019, 11, 29),
    catchup=False
    )

params = {
    "dm_base_dir" : "/opt/dataplatform2/mj/datamart/",
}


to_exclude = ["apply_calculated_sales_and_cost.sql", "create_questionnaires.sql", "update_questionnaires.sql"]
sql_files = [file for file in os.listdir(params["dm_base_dir"]) if ".sql" in file and file not in to_exclude]

tasks  = {}

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

for sql_file in sql_files:
    with open(params['dm_base_dir'] + sql_file) as f:
        tasks[sql_file[:-4]]= BigQueryOperator(
            dag = dag,
            task_id = sql_file[:-4],
            bigquery_conn_id = "bigquery", 
            use_legacy_sql = False, 
            location = "asia-northeast1",
            sql = f.read(), 
            allow_large_results = True
        )

start >> tasks['fix_mj_events'] >> tasks['fix_mj_users'] >> [
    tasks['events'], 
    tasks['users'], 
    tasks['purchases'], 
    tasks['check_in_tickets'], 
    tasks['subscriptions'],
    tasks['daily_event_pvcv'], 
    tasks['fill_normalized_url']
    ] >> tasks['fix_mj_dm_events'] >> tasks['update_ga_columns'] >> end