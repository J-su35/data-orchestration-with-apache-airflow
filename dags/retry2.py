# retry2.py
from datetime import timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}
with DAG(
    default_args=default_args,
):
    BashOperator(bash_command="echo I get 3 retries! && False", retries=5, retry_delay=timedelta(seconds=5),)