from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_Python():
    print("test_python")

with DAG (
    dag_id="first_DAG",
    default_args = {"start_date": datetime(2026,1,1)},
    schedule_interval ="@daily",
) as dag:
    start = DummyOperator(
        task_id="start",
    )

    bash_task = BashOperator(
        task_id="print_bash",
        bash_command="echo test_bash",
    )

    python_task = PythonOperator(
        task_id="print_python",
        python_callable=print_Python,
    )

    end = DummyOperator(
        task_id="end",
    )

    start >> bash_task >> python_task >> end