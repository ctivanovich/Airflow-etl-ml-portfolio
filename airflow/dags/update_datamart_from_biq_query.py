import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from ..modules.slack_alert import slack_alert, start_alert, end_alert

dag = DAG(  
    dag_id="update-dm-bq",
    description="Importation of masked data into Data Mart", 
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
    "dm_base_dir" : "/..../",
}


to_exclude = ["a list of some tables not currently in use"]
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
            location = "....",
            sql = f.read(), 
            allow_large_results = True
        )

dummy_node = DummyOperator(
    dag = dag,
    task_id = 'transit_point'
    )

### this is a complex etl job with dependencies that require execution in this order
### the first set drop tables and then dump data, and the further steps clean or modify this data
start >> tasks['sql1'] >> tasks['sql2'] >> [
    tasks['sql3'], 
    tasks['sql4'], 
    tasks['sql5']
    ] >> dummy_node >> [
    tasks['sql6'], 
    tasks['sql7'], 
    tasks['sql8'], 
    tasks['sql9'],
    tasks['sql10'], 
    tasks['sql11']
    ] >> tasks['sql12'] >> tasks['sql13'] >> tasks['sql14'] >> end