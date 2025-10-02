from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def _hello():
    print("Hello")

with DAG(
    "everyday_lottoday",
    start_date=timezone.datetime(2025, 10, 2),
    schedule="30 17 1,16 * *",
):
    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    lotto = BashOperator(
        task_id = "lotto",
        bash_command="echo 'lotto day'",
    )
    
    hello >> lotto
    