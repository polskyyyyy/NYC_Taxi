import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'b.polskii',
    'start_date': dt.datetime(2024, 12, 1),
}

@dag(default_args=default_args, schedule_interval='@daily', dag_id='api_dag')
def api_dag():
    @task
    def even_only():
        context = get_current_context()
        execution_date = context['execution_date']

        if execution_date.day % 2 != 0:
            raise ValueError(f'Odd day {execution_date}')

    @task
    def dummy_task():
        pass

    even_only() >> dummy_task


dag_instance = api_dag()
