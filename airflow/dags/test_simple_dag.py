from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def my_first_function():
    print("Hello from my first Airflow task!")

def my_second_function():
    print("Hello from my second Airflow task!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'test_simple_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    task1 = PythonOperator(
        task_id='first_task',
        python_callable=my_first_function,
    )

    task2 = PythonOperator(
        task_id='second_task',
        python_callable=my_second_function,
    )

  
    task1 >> task2