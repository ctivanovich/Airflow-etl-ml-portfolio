import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceImportOperator, CloudSqlQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from slack_alert import slack_alert, start_alert, end_alert
from modules.ETL_csv_files import ETL_csv_files

GCS_BUCKET_NAME = '.....'
DATASET = '.....'
DDL_DIR = '.....'

dag = DAG(
    dag_id='dump-dm-to-cloud-sql',
    description='Dumping BQ datamart into secondary cloud sql instance', 
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
        'on_failure_callback': slack_alert
        },
    schedule_interval='0 9 * * *', 
    start_date=datetime(2020, 1, 20),
    catchup=False
)

tables = [
   '....',
   '....',
   '....'
]

def subdag(table, parent_dag, start_date, schedule_interval):
    # The subdag's dag_id should have the form '{parent_dag_id}.{this_task_id}'. 
    dag = DAG(
        dag_id = f'{parent_dag}.{table}-subdag',
        schedule_interval=schedule_interval,
        start_date=start_date,
        default_args = {
            'retries': 2,
            'retry_delay': timedelta(minutes=1),
        }
    )
    t1 = BigQueryToCloudStorageOperator(
        dag = dag,
        bigquery_conn_id = '......',
        location = '......',
        task_id = f'dump-{table}',
        print_header = False,
        export_format = 'csv',
        source_project_dataset_table = DATASET + '.' + table,
        destination_cloud_storage_uris = \
            'gs://' + GCS_BUCKET_NAME + '/' + f'{table}/*.csv'
    ) 
    t2 = PythonOperator(
        dag = dag,
        task_id = f'ETL-GCS-{table}.csv',
        python_callable = ETL_csv_files,
        op_args = [table, GCS_BUCKET_NAME]
    ) 
    t3 = MySqlOperator(
        dag = dag,
        task_id = f'drop-{table}',
        mysql_conn_id = '....',
        database = DATASET,
        sql = f'DROP TABLE IF EXISTS {table};'
    ) 
    t4 = MySqlOperator(
        dag = dag,
        task_id = f'create-{table}',
        mysql_conn_id = '....',
        database = DATASET,
        sql = open(DDL_DIR + table + '.sql', 'r').read()
    )
    t5 = BashOperator(
        dag = dag,
        task_id = f'import-{table}',
        params = {
            'database' : DATASET,
            'defaults_file' : DDL_DIR + 'my.cnf',
            'working_dir' : '/tmp/',
            'source_files' : 'temp-' + GCS_BUCKET_NAME + '_' + table + '-*.csv',
            'log_file' : 'temp-' + GCS_BUCKET_NAME + '_' + table + '.log',
            'tables_dir' : '/tmp/' + GCS_BUCKET_NAME + '_' + table + '/',
            'table_file' : table + '.csv'
        },
        bash_command = '''
        A bash command that performs the dump into mysql using the mysql utility for Linux;
        it also performs logging and clean up, as all of these files are managed within a container, 
        and not persisted to the local volume.
        '''
    )
    t1 >> t2 >> t3 >> t4 >> t5
    return dag

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

subdags = [start, end]

for table in tables:
    subdags.insert(
        -1,
        SubDagOperator(
            subdag = subdag(
                table,
                dag.dag_id,
                dag.start_date, 
                dag.schedule_interval, 
            ),
            task_id = f'{table}-subdag',
            dag = dag,
        )
    )

for i in range(0, len(subdags) - 1):
    subdags[i] >> subdags[i+1]
