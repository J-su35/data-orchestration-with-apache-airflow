# retry.py
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
}
with DAG(
    "retry",
    start_date=datetime(2025, 10, 1),
    default_args=default_args,
):
    task_1 = BashOperator(task_id="task_1", bash_command="echo I get 3 retries! && False")