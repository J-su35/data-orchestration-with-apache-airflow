# variable.py
import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable


def print_version():
    version = Variable.get("version")
    print(f"Version: {version}")


with DAG(
    dag_id="variable",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")

    echo_env = BashOperator(
        task_id="echo_env",
        bash_command="echo 'Version: {{ var.value.version }}'",
    )

    print_version = PythonOperator(
        task_id="print_version",
        python_callable=print_version,
    )

    end = EmptyOperator(task_id="end")

    start >> [echo_env, print_version] >> end