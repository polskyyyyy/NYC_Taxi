from datetime import datetime
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.http.operators.http import SimpleHttpOperator
from .functions import download_dataset, convert_to_parquet

default_args = {
    'owner': 'b.polskii',
}

@dag(
    default_args=default_args,
    schedule_interval='@mountly',
    dag_id='taxi_dag',
    start_date=datetime(2024, 9, 1),
)
def taxi_dag():

    check_file = SimpleHttpOperator(
        task_id='check_file',
        method='HEAD',
        http_conn_id='nyc_yellow_taxi_id',
        endpoint='yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv'
    )

    @task
    def download_file():
        context = get_current_context()
        return download_dataset(context['execution_date'].strftime('%Y-%m'))

    @task
    def to_parquet(file_path: str):
        context = get_current_context()
        return convert_to_parquet(context['execution_date'].strftime('%Y-%m'), file_path)
    


    file_path = download_file()
    parquet_file_path = to_parquet(file_path)

    check_file >> file_path >> parquet_file_path


nyc_taxi_dataset_dag = taxi_dag()