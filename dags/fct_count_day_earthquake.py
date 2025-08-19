import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "SterkhovAS"
DAG_ID = "fct_count_day_earthquake"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_count_day_earthquake"

# DWH
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Описание DAG: fct_count_day_earthquake

## Цель
Этот DAG предназначен для ежедневного подсчёта количества землетрясений из слоя `ods` в целевую таблицу `dm.fct_count_day_earthquake` в дата-warehousе. Основная задача — определение общего числа событий землетрясений для каждого дня на основе данных из таблицы `ods.fct_earthquake`.

## Контекст
- **Владелец**: SterkhovAS
- **Слой данных**: raw → ods → dm
- **Источник**: Данные о землетрясениях (earthquake)
- **Целевая таблица**: `dm.fct_count_day_earthquake`
- **Подключение**: postgres_dwh

DAG запускается ежедневно в 5:00 утра (по расписанию `0 5 * * *`) с учётом часового пояса Europe/Moscow и поддерживает catchup для обработки пропущенных периодов с 1 мая 2025 года.

## Шаги выполнения
1. **Начало (start)**: Инициализация DAG.
2. **Сенсор (sensor_on_raw_layer)**: Ожидание успешного завершения DAG `raw_from_s3_to_pg`, который загружает данные в слой `raw`.
3. **Удаление временной таблицы (drop_stg_table_before)**: Удаление старой временной таблицы `stg.tmp_fct_count_day_earthquake_<дата>`, если она существует.
4. **Создание временной таблицы (create_stg_table)**: Создание временной таблицы с агрегированными данными (дата и количество событий) за указанный день.
5. **Удаление старых данных в целевой таблице (drop_from_target_table)**: Удаление записей из `dm.fct_count_day_earthquake`, соответствующих дате из временной таблицы.
6. **Вставка данных в целевую таблицу (insert_into_target_table)**: Перенос агрегированных данных из временной таблицы в `dm.fct_count_day_earthquake`.
7. **Удаление временной таблицы (drop_stg_table_after)**: Очистка временной таблицы после успешной вставки.
8. **Окончание (end)**: Завершение DAG.

## Технические детали
- **Расписание**: Ежедневно в 5:00 утра (Europe/Moscow).
- **Дата начала**: 1 мая 2025 года.
- **Повторные попытки**: 3 попытки с интервалом 1 час.
- **Теги**: dm, pg.
- **Ограничения**: Concurrency=1, max_active_tasks=1, max_active_runs=1.
"""

SHORT_DESCRIPTION = "Подсчёт количества землетрясений по дням"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["dm", "pg"],
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
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            count(*)
        FROM
            ods.fct_earthquake
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1
        """,
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
            start >>
            sensor_on_raw_layer >>
            drop_stg_table_before >>
            create_stg_table >>
            drop_from_target_table >>
            insert_into_target_table >>
            drop_stg_table_after >>
            end
    )