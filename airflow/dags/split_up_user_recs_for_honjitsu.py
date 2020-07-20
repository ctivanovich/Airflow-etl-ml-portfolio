from airflow.modules.subset_and_deliver_recs import subset_and_deliver_daily_recommendations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from modules.slack_alert import slack_alert, start_alert, end_alert

from modules.subset_and_deliver_recs import subset_and_deliver_honjitsu_no_osusume

URI_BASE = 'gs://cl-personalization.datasets.linkbal.com/outputs/'
HONJITSU_FOLDER = URI_BASE + 'honjitsu_no_osusume/'
TAB_FOLDER = URI_BASE + 'osusume_tab/'

IMAGE_RECS_BASE = 'image_clustering_v1_district_{}.csv'
IMPLICIT_RECS_BASE = 'implicit_svd_v2_district_{}.csv'
LIGHTFM_RECS_BASE = 'lightfm_v1_district_{}.csv'

rec_methods_map = {
    'implicit': URI_BASE + IMPLICIT_RECS_BASE,
    'image': URI_BASE + IMAGE_RECS_BASE,
    'lightfm': URI_BASE + LIGHTFM_RECS_BASE
}

honjitsu_destinations = {
    'implicit': HONJITSU_FOLDER + 'implicit_svd_v2.csv',
    'image': HONJITSU_FOLDER + 'image_clustering_v1.csv',
    'lightfm': HONJITSU_FOLDER + 'lightfm_v1.csv'
}

tab_destinations = {
    'implicit': TAB_FOLDER + 'implicit_svd_v2.csv',
    'image': TAB_FOLDER + 'image_clustering_v1.csv',
    'lightfm': TAB_FOLDER + 'lightfm_v1.csv'
}

dag = DAG(  
    dag_id="split-recommendations-for-honjitsu-no-osusume",
    description="recommendation separation for our two entrypoints, for each method, regardless of district", 
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack_alert
        },
    schedule_interval="0 17 * * *", 
    start_date=datetime(2020, 4, 15),
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

def print_context(ds, **kwargs):
    from pprint import pprint
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


log_to_UI = PythonOperator(
    task_id='output-context-to-log',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

split_recommendations = PythonOperator(
    task_id = 'split-for-daily-recommendations',
    python_callable = subset_and_deliver_daily_recommendations,
    op_kwargs = {
        'rec_methods_map': rec_methods_map, 
        'honjitsu_destinations': honjitsu_destinations,
        'tab_destinations' :tab_destinations
    }
)

start >> log_to_UI >> split_recommendations >> end