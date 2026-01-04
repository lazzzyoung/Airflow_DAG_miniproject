from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
import random

dag = DAG(
    'advanced_dag',
    schedule_interval='0 9 * * *',
    start_date=datetime(2026,1,1),
    catchup=False,
)

def randomNumber():
    number = random.randint(1,10)
    print(f"Random Number : {number}")

def branch():
    number = random.randint(1,10)
    print(f"Branch Number : {number}")

    if(number > 4):
        return 'high_task'
    else:
        return 'low_task'

def high_task():
    print("Random number is High Number")

def low_task():
    print("Random number is low Number")

def final_task():
    print("Final Task")


start_task = PythonOperator(
    task_id="Create_RandNumber_task",
    python_callable=randomNumber,
    dag=dag
)

branch_task = BranchPythonOperator(
    task_id="High_low_branch",
    python_callable=branch,
    dag=dag
)

high_task = PythonOperator(
    task_id="high_task",
    python_callable=high_task,
    dag=dag
)

low_task = PythonOperator(
    task_id="low_task",
    python_callable=low_task,
    dag=dag
)

final_task = PythonOperator(
    task_id="final_task",
    trigger_rule='all_done',
    python_callable=final_task,
    dag=dag
)
slack_notify = SlackWebhookOperator(
    task_id="slack_alert_task",
    slack_webhook_conn_id="slack_conn",
    trigger_rule='all_done',
    message="DAG 작업 완료",
    channel="#airflow",
    dag=dag
)



start_task >> branch_task
branch_task >> [high_task, low_task]
[high_task,low_task] >> slack_notify >> final_task