import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "SterkhovAS"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# Описание DAG: raw_from_api_to_s3

## Цель
Этот DAG предназначен для ежедневной загрузки данных о землетрясениях с публичного API USGS (U.S. Geological Survey) и их переноса в слой `raw` хранилища S3 (Minio) в формате Parquet. Данные агрегируются за указанный интервал дат и сохраняются для дальнейшей обработки в аналитической системе.

## Контекст
- **Владелец**: SterkhovAS
- **Слой данных**: raw
- **Источник**: Данные о землетрясениях (earthquake) с API USGS
- **Хранилище**: S3 (Minio) по адресу `s3://prod/raw/earthquake/<дата>/<дата>_00-00-00.gz.parquet`
- **Подключение**: Используются переменные окружения `access_key` и `secret_key` для аутентификации в Minio

DAG запускается ежедневно в 5:00 утра (по расписанию `0 5 * * *`) с учётом часового пояса Europe/Moscow и поддерживает catchup для обработки пропущенных периодов с 1 мая 2025 года.

## Шаги выполнения
1. **Начало (start)**: Инициализация DAG.
2. **Загрузка и перенос данных (get_and_transfer_api_data_to_s3)**: 
   - Получение данных с API USGS за указанный интервал дат (от `data_interval_start` до `data_interval_end`).
   - Использование DuckDB для обработки данных и их сохранения в S3 в формате Parquet с сжатием GZIP.
   - Логирование начала и успешного завершения процесса.
3. **Окончание (end)**: Завершение DAG.

## Технические детали
- **Расписание**: Ежедневно в 5:00 утра (Europe/Moscow).
- **Дата начала**: 1 мая 2025 года.
- **Повторные попытки**: 3 попытки с интервалом 1 час.
- **Теги**: s3, raw.
- **Ограничения**: Concurrency=1, max_active_tasks=1, max_active_runs=1.
- **Инструменты**: DuckDB для обработки, Minio как S3-совместимое хранилище.
"""

SHORT_DESCRIPTION = "Загрузка данных о землетрясениях с API в S3"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';

        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end