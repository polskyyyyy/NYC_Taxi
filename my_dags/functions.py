import requests
import gzip
import pandas as pd
import datetime as dt

from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook
from tempfile import NamedTemporaryFile


def download_file(year_month: str):
    url = (
        f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year_month}.csv'
    )
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    hdfs_hook = HDFSHook(hdfs_conn_id='polskii_hdfs_default')  # ID вашего подключения
    hdfs_client = hdfs_hook.get_conn()

    # Создаем временный файл для загрузки данных
    with NamedTemporaryFile(delete=False) as temp_file:
        for chunk in response.iter_content(chunk_size=1024):
            temp_file.write(chunk)
        temp_file.flush()

        # Определяем путь на HDFS
        hdfs_path = f'hdfs://rc1a-dataproc-m-ucdvdhi2gxsxj4y9.mdb.yandexcloud.net/user/b.polskii/data/yellow_tripdata_{year_month}.parquet'

        # Загружаем файл на HDFS
        with open(temp_file.name, 'rb') as local_file:
            hdfs_client.write(hdfs_path, local_file, overwrite=True)

    return hdfs_path


def convert_to_parquet(year_month: str, hdfs_path: str):
    # Подключение к HDFS
    hdfs_hook = HDFSHook(hdfs_conn_id='polskii_hdfs_default')
    hdfs_client = hdfs_hook.get_conn()

    # Загрузка файла из HDFS
    with hdfs_client.open(hdfs_path, 'rb') as f:
        df = pd.read_csv(f)

    # Преобразование данных по месяцу
    current_month = dt.datetime.strptime(year_month, '%Y-%m')
    next_month = current_month.replace(month=current_month.month + 1)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df = df[
        (df['tpep_pickup_datetime'] >= f'{current_month:%Y-%m-%d}') &
        (df['tpep_pickup_datetime'] < f'{next_month:%Y-%m-%d}')
        ]
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')

    # Путь к файлу на HDFS
    hdfs_parquet_path = f'/user/b.polskii/data/yellow_tripdata_{year_month}.parquet'

    # Сохранение файла в формат Parquet во временный файл
    with NamedTemporaryFile('wb', delete=False) as temp_file:
        df.to_parquet(temp_file)

        # Запись файла в HDFS
        hdfs_client.write(hdfs_parquet_path, temp_file, overwrite=True)

    return hdfs_parquet_path
