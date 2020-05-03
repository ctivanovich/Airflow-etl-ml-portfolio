import pytz

from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def slack_alert(context):
    task_instance = context.get('task_instance')
    state = task_instance.state

    def build_log_url(url):
        import re
        airflow_base_url = 'localhost:8080/admin/airflow/'
        log = re.search('(?P<log>[l][o][g].*)', url)['log']
        return airflow_base_url + log

    slack_webhook_token = BaseHook.get_connection(conn_id = "slack").password
    if state == 'success':
        slack_msg = """
        :ok_woman: Task Success!
        *Task*: {task}  
        *Dag*: {dag}""".format(
            task = task_instance.task_id,
            dag = task_instance.dag_id,
            )
    else: 
        slack_msg = """
        :no_good: Task Failed. 
        *Task*: {task}  
        *Dag*: {dag} 
        *Log Url*: {log_url}""".format(
            task = task_instance.task_id,
            dag = task_instance.dag_id,
            log_url = build_log_url(task_instance.log_url),
            )
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='...',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='...')
    return alert.execute(context=context)


### for use with PythonOperator, as they require passing in of the context
def start_alert(**context):
    slack_webhook_token = BaseHook.get_connection(conn_id = "...").password
    task_instance = context.get('task_instance')
    start_time = datetime.now().astimezone(pytz.timezone('Asia/Tokyo'))
    slack_msg = f"""
    :man-gesturing-ok: Beginning dag run:
    *Dag*: {task_instance.dag_id}
    *Start time*: {start_time.strftime("%H:%M:%S")}
    """
    alert = SlackWebhookOperator(
        task_id='dag_alert',
        http_conn_id='...',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='...')
    return alert.execute(context=context)

def end_alert(**context):
    slack_webhook_token = BaseHook.get_connection(conn_id = "...").password
    end_time = datetime.now().astimezone(pytz.timezone('Asia/Tokyo'))
    task_instance = context.get('task_instance')
    slack_msg = f"""
    :man-bowing: Dag run completed:
    *Dag*: {task_instance.dag_id}
    *End time*: {end_time.strftime("%H:%M:%S")}
    """
    alert = SlackWebhookOperator(
        task_id='dag_alert',
        http_conn_id='...',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='...')
    return alert.execute(context=context)
