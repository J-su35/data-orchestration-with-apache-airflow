from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def _hello():
    print("Hello")

with DAG(
    "everysunday",
    start_date=timezone.datetime(2025, 10, 2),
    schedule="0 0 * * 0",
):
    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    sunday = BashOperator(
        task_id = "sunday",
        bash_command="echo 'Sunday'",
    )
    
    hello >> sunday
    