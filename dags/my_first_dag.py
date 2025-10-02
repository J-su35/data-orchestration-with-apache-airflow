from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    "my_first_dag",
    start_date=timezone.datetime(2025, 10, 2),
    schedule=None,
):
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t2 >> t1