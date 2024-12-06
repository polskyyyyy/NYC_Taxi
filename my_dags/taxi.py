from datetime import datetime
import requests
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.http.operators.http import SimpleHttpOperator
from aiflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from tempfile import NamedTemporaryFile
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa



default_args = {
    'owner': 'b.polskii',
}

def download_dataset(year_month: str):
    """
    Download a NYC yellow taxi dataset from a public source.

    Args:
        year_month (str): Year and month of the dataset to be downloaded.

    Returns:
        str: HDFS path of the downloaded dataset.
    """
    url = (
        f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet'
    )
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    hdfs_hook = WebHDFSHook(webhdfs_conn_id='polskii_hdfs_default')
    hdfs_client = hdfs_hook.get_conn()

    # Создаем временный файл для загрузки данных
    with NamedTemporaryFile(delete=False) as temp_file:
        for chunk in response.iter_content(chunk_size=1024):
            temp_file.write(chunk)
        temp_file.flush()

        # Определяем путь на HDFS
        hdfs_path = f'/user/b.polskii/data/yellow_tripdata_{year_month}.parquet'

        # Загружаем файл на HDFS
        with open(temp_file.name, 'rb') as local_file:
            hdfs_client.write(hdfs_path, local_file, overwrite=True)

    return hdfs_path


def merge_parquet_files(hdfs_directory: str, output_file: str):

    # Инициализация клиента HDFS через WebHDFSHook
    """
    Merge multiple Parquet files into a single one.

    The function takes a list of Parquet files, reads them into DataFrames,
    concatenates them into a single DataFrame and writes it back to a single
    Parquet file.

    Args:
        hdfs_directory (str): HDFS directory containing Parquet files to be merged.
        output_file (str): HDFS path of the output Parquet file.

    Returns:
        str: HDFS path of the output Parquet file.

    Raises:
        ValueError: If no Parquet files are found in the directory.
    """
    hdfs_hook = WebHDFSHook(webhdfs_conn_id='polskii_hdfs_default')
    hdfs_client = hdfs_hook.get_conn()

    # Получение списка всех файлов в указанной директории
    files = hdfs_client.list(hdfs_directory)
    parquet_files = [f"{hdfs_directory}/{file}" for file in files if file.endswith('.parquet')]

    if not parquet_files:
        raise ValueError(f"No Parquet files found in directory: {hdfs_directory}")

    # Список для хранения датафреймов
    dataframes = []

    for parquet_file in parquet_files:
        # Чтение содержимого файла с HDFS
        with hdfs_client.read(parquet_file) as file_reader:
            file_content = file_reader.read()
            
            # Временный файл для чтения как DataFrame
            with NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file.flush()
                # Преобразование в DataFrame
                df = pd.read_parquet(temp_file.name)
                dataframes.append(df)

        # Удаление файла из HDFS после его обработки
        hdfs_client.delete(parquet_file)

    # Объединяем все датафреймы
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Сохранение объединённого файла Parquet
    with NamedTemporaryFile(delete=False) as temp_file:
        combined_df.to_parquet(temp_file.name, index=False)
        # Загрузка объединённого файла на HDFS
        with open(temp_file.name, 'rb') as local_file:
            hdfs_client.write(output_file, local_file, overwrite=True)

    return output_file




@dag(
    default_args=default_args,
    schedule_interval='@monthly',
    dag_id='taxi_dag',
    start_date=datetime(2024, 3, 1),
    # end_date=datetime(2024, 9, 1),
)
def taxi_dag():

    check_file = HttpSensor(
        task_id='check_file',
        method='HEAD',
        http_conn_id='nyc_yellow_taxi_id',
        endpoint='yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}.parquet',
        poke_interval=60 * 60 * 24 * 31,
        mode='reschedule',
    )

    @task
    def download_file():        
        """
        Downloads a NYC yellow taxi dataset from a public source using the current date.

        Args:
            None

        Returns:
            str: HDFS path of the downloaded dataset.
        """
        context = get_current_context()
        return download_dataset(context['execution_date'].strftime('%Y-%m'))

    @task
    def check_and_merge(year_month: str):
        """
        Checks if the main file exists and either renames the new file as main or merges the new file with the main one.

        Args:
            year_month (str): Year and month of the dataset to be checked.

        Returns:
            str: HDFS path of the merged dataset.
        """
        hdfs_directory = '/user/b.polskii/data'
        main_file = f'{hdfs_directory}/yellow_tripdata_main.parquet'
        new_file = f'{hdfs_directory}/yellow_tripdata_{year_month}.parquet'

        # Проверим, существует ли основной файл
        hdfs_hook = WebHDFSHook(webhdfs_conn_id='polskii_hdfs_default')
        hdfs_client = hdfs_hook.get_conn()

        try:
            # Проверка существования основного файла с использованием метода 'status'
            hdfs_client.status(main_file)
            main_file_exists = True
        except:
            main_file_exists = False

        # Если основной файл отсутствует, то новый файл становится основным
        if not main_file_exists:
            # Переименуем новый файл в основной
            hdfs_client.rename(new_file, main_file)
            return main_file
        else:
            # Если основной файл существует, сливаем новый файл с основным
            merged_file = f'{hdfs_directory}/merged_yellow_tripdata_{year_month}.parquet'
            merge_parquet_files(hdfs_directory, merged_file)
            return merged_file


    # Выполнение задач
    file_path = download_file()
    merged_or_main_file = check_and_merge('{{ execution_date.strftime("%Y-%m") }}')

    # Зависимости
    check_file >> file_path >> merged_or_main_file

# Запуск DAG
nyc_taxi_dataset_dag = taxi_dag()
