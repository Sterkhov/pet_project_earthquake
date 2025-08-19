import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "SterkhovAS"
DAG_ID = "raw_from_s3_to_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# Описание DAG: raw_from_s3_to_pg

## Цель
Этот DAG предназначен для ежедневного переноса сырых данных о землетрясениях из слоя `raw` хранилища S3 (Minio) в таблицу `ods.fct_earthquake` в дата-warehousе PostgreSQL. Данные загружаются за указанный интервал дат и преобразуются для дальнейшей обработки в аналитической системе.

## Контекст
- **Владелец**: SterkhovAS
- **Слой данных**: raw → ods
- **Источник**: Данные о землетрясениях (earthquake) из S3 (`s3://prod/raw/earthquake/<дата>/<дата>_00-00-00.gz.parquet`)
- **Целевая таблица**: `ods.fct_earthquake`
- **Подключение**: PostgreSQL через `postgres_dwh` с использованием переменной `pg_password` для аутентификации

DAG запускается ежедневно в 5:00 утра (по расписанию `0 5 * * *`) с учётом часового пояса Europe/Moscow и поддерживает catchup для обработки пропущенных периодов с 1 мая 2025 года. Он зависит от успешного завершения DAG `raw_from_api_to_s3`.

## Шаги выполнения
1. **Начало (start)**: Инициализация DAG.
2. **Сенсор (sensor_on_raw_layer)**: Ожидание успешного завершения DAG `raw_from_api_to_s3`, который загружает данные в S3.
3. **Перенос данных (get_and_transfer_raw_data_to_ods_pg)**:
   - Подключение к S3 через DuckDB с использованием `access_key` и `secret_key`.
   - Чтение данных из Parquet-файла за указанную дату.
   - Преобразование имён столбцов (например, `magType` в `mag_type`) и вставка в таблицу `ods.fct_earthquake` в PostgreSQL.
   - Логирование начала и успешного завершения процесса.
4. **Окончание (end)**: Завершение DAG.

## Технические детали
- **Расписание**: Ежедневно в 5:00 утра (Europe/Moscow).
- **Дата начала**: 1 мая 2025 года.
- **Повторные попытки**: 3 попытки с интервалом 1 час.
- **Теги**: s3, ods, pg.
- **Ограничения**: Concurrency=1, max_active_tasks=1, max_active_runs=1.
- **Инструменты**: DuckDB для обработки, PostgreSQL как целевая база, Minio как S3-совместимое хранилище.
"""

SHORT_DESCRIPTION = "Перенос данных о землетрясениях из S3 в PostgreSQL"

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


def get_and_transfer_raw_data_to_ods_pg(**context):
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

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontal_error,
            depth_error,
            mag_error,
            mag_nst,
            status,
            location_source,
            mag_source
        )
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end