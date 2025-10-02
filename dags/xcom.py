# xcom.py
import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _push(ti):
    ti.xcom_push(key="animal", value="cat")
    return "tiger"


def _pull(ti):
    animal = ti.xcom_pull(task_ids="push", key="animal")
    print(f"This is a {animal}!")

    another_animal = ti.xcom_pull(task_ids="push", key="return_value")
    print(f"This is a {another_animal}!")


with DAG(
    dag_id="xcom",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")

    push = PythonOperator(
        task_id="push",
        python_callable=_push,
    )

    pull = PythonOperator(
        task_id="pull",
        python_callable=_pull,
    )

    end = EmptyOperator(task_id="end")

    start >> push >> pull >> end