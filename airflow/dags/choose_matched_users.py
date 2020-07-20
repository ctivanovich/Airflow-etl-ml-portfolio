from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from ..modules.slack_alert import slack_alert, start_alert, end_alert
from ..modules.choose_matched_users_utils import *

URI_BASE = ''
RECOMMENDATIONS_FOLDER = URI_BASE + ''
K = 3

recs_uris = {
    'images': URI_BASE + 'image_clustering_v1.csv',
    'implicit': URI_BASE + 'implicit_svd_v2.csv',
    'lightfm':  URI_BASE + 'lightfm_v1.csv'
}

dag = DAG(  
    dag_id='choose-matched-users',
    description='searching through compiled and separated recs in search of male/female model output agreement', 
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack_alert
        },
    schedule_interval='15 17 * * *', 
    start_date=datetime(2020, 7, 1),
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
    
subdags = []

def subdag(name, recs_uri, parent_dag, start_date, schedule_interval):
    # The subdag's dag_id should have the form '{parent_dag_id}.{this_task_id}'.
    dag = DAG(
        dag_id = f'{parent_dag}.{name}-subdag',
        schedule_interval=schedule_interval,
        start_date=start_date,
        default_args = {
            'retries': 2,
            'retry_delay': timedelta(minutes=1)
        }
    )
    find_matches = PythonOperator(
        dag = dag,
        task_id = f'find_matches_{name}',
        python_callable = find_match_agreement,
        provide_context = False,
        op_kwargs = {
            'recs_df_uri' : recs_uri,
            'k' : K
        }
    )
    obtain_new_indices = PythonOperator(
        dag = dag,
        task_id = f'obtain_new_indices_{name}',
        python_callable = insert_and_recreate_index,
        provide_context = True,
        op_kwargs = {
            'prior_task':f'find_matches_{name}'
        }
    )
    deliver_reindexed = PythonOperator(
        dag = dag,
        task_id = f'deliver_reindexed_{name}',
        python_callable = reindex_and_deliver_df,
        provide_context = True,
        op_kwargs = {
            'recs_df_uri': recs_uri, 
            'prior_task':f'obtain_new_indices_{name}'
        }
    )
    clear_xcoms = PostgresOperator(
        dag = dag,
        task_id = f'clear_xcoms_{name}',
        postgres_conn_id = 'airflow_db',
        sql = """delete from xcom where dag_id = '{{ dag.dag_id }}' and execution_date::DATE='{{ ds }}'"""
    )
    find_matches >> obtain_new_indices >> deliver_reindexed >> clear_xcoms
    return dag

for name, recs_uri in recs_uris.items():
    subdags.append(
        SubDagOperator(
            subdag = subdag(
                name,
                recs_uri,
                dag.dag_id,
                dag.start_date, 
                dag.schedule_interval, 
            ),
            task_id = f'{name}-subdag',
            dag = dag,
        )
    )

start >> subdags >> end