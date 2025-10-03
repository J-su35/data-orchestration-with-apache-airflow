import os
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

BUCKET_NAME = "pea-watt"


@dag(
    start_date=datetime(2025, 10, 1),
)
def etl():
    start = EmptyOperator(task_id="start")

    @task
    def prepare_tmp_folder(ds):
        base_folder = f"/tmp/etl/{ds}"

        os.makedirs(f"{base_folder}/orders", exist_ok=True)
        os.makedirs(f"{base_folder}/customers", exist_ok=True)

        return base_folder

    prepare_tmp_folder_task = prepare_tmp_folder()

    @task
    def extract_orders_from_source(base_folder):
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        df = pg_hook.get_df("SELECT * FROM orders;")

        file_path = f"{base_folder}/orders/orders.parquet"
        df.to_parquet(file_path)

        return file_path

    extract_orders_from_source_task = extract_orders_from_source(
        prepare_tmp_folder_task
    )

    @task
    def load_orders_to_landing(source_filepath, ds):
        destination_filepath = f"jiranat/etl/{ds}/orders.parquet"
        s3_hook = S3Hook(aws_conn_id="my_aws_connection")
        s3_hook.load_file(
            filename=source_filepath,
            key=destination_filepath,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

    load_orders_to_landing_task = load_orders_to_landing(
        extract_orders_from_source_task
    )

    @task
    def extract_customers_from_source(base_folder):
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        df = pg_hook.get_df("SELECT * FROM customers;")

        file_path = f"{base_folder}/customers/customers.parquet"
        df.to_parquet(file_path)

        return file_path

    extract_customers_from_source_task = extract_customers_from_source(
        prepare_tmp_folder_task
    )

    @task
    def transform_customers(source_filepath, task_instance):
        import pandas as pd

        base_folder = task_instance.xcom_pull(
            task_ids="prepare_tmp_folder", key="return_value"
        )

        df = pd.read_parquet(source_filepath)
        df["birthdate"] = pd.to_datetime(df["birthdate"], dayfirst=True)

        file_path = f"{base_folder}/customers/customers-transformed.parquet"
        df.to_parquet(file_path)

        return file_path

    transform_customers_task = transform_customers(extract_customers_from_source_task)

    @task
    def load_customers_to_landing(source_filepath, ds):
        destination_filepath = f"jiranat/etl/{ds}/customers.parquet"
        s3_hook = S3Hook(aws_conn_id="my_aws_connection")
        s3_hook.load_file(
            filename=source_filepath,
            key=destination_filepath,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

    load_customers_to_landing_task = load_customers_to_landing(transform_customers_task)

    @task
    def delete_tmp_folder(base_folder):
        import shutil

        shutil.rmtree(base_folder, ignore_errors=True)

    delete_tmp_folder_task = delete_tmp_folder(prepare_tmp_folder_task)

    end = EmptyOperator(task_id="end")

    start >> prepare_tmp_folder_task
    load_orders_to_landing_task >> delete_tmp_folder_task
    load_customers_to_landing_task >> delete_tmp_folder_task
    delete_tmp_folder_task >> end


etl()
