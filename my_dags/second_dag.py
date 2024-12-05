import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'b.polskii',
    'start_date': dt.datetime(2024, 12, 1),
}

def even_only():
    """
    Raises a ValueError if the execution date day is odd.

    Args:
        context (dict): The context dictionary containing task instance information,
                        including 'execution_date'.

    Raises:
        ValueError: If the day of the execution date is odd.
    """
    context = get_current_context()
    execution_date = context['execution_date']
    if execution_date.day % 2 != 0:
        raise ValueError(f'Odd day: {execution_date}')

with DAG(
    dag_id='second_dag',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    
    even_only = PythonOperator(
        task_id='even_only',
        python_callable=even_only,
        dag=dag,
    )

    dummy  = DummyOperator(
        task_id='dummy_task',
        dag=dag,
    )

    even_only >> dummy