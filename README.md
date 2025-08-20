Добро пожаловать в проект `pet_project_earthquake`! Это личный проект, посвящённый сбору, обработке и анализу данных о землетрясениях с использованием современных инструментов автоматизации и аналитики данных. Проект включает пайплайн ETL (Extract, Transform, Load) для загрузки данных с API USGS, их хранения в MinIO и агрегации в PostgreSQL.

## О проекте

Проект разработан для автоматизации обработки данных о землетрясениях, начиная с 1 мая 2025 года. Он включает следующие ключевые этапы:
- Загрузка сырых данных с API USGS в объектное хранилище MinIO.
- Перенос данных из MinIO в PostgreSQL с использованием DuckDB.
- Агрегация данных (средняя магнитуда и количество событий) с помощью Apache Airflow.

Цель проекта — создать надёжную аналитическую платформу для исследования сейсмической активности.

Архитектура - **Data Lakehouse**.

Не применяется **звезда**, **снежинка** или другое, потому что в этом нет необходимости. Данных немного. Состояние измениться не
может, поэтому создаём модель по типу "_AS IS_".

## Требования

- **Python**: Версия 3.11
- **Apache Airflow**: Для оркестрации DAG
- **PostgreSQL**: Для хранения агрегированных данных
- **MinIO**: S3-совместимое хранилище для сырых данных
- **DuckDB**: Для обработки данных
- **Git**: Для управления версиями

## Установка

### Клонирование репозитория
```bash
git clone https://github.com/Sterkhov/pet_project_earthquake.git
cd pet_project_earthquake

## Создание виртуального окружения

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt
```

## Разворачивание инфраструктуры

```bash
docker-compose up -d
```

## Ссылки

- [Описание работы API](https://earthquake.usgs.gov/fdsnws/event/1/#methods)
- [Описание полей из API](https://earthquake.usgs.gov/data/comcat/index.php)
- [airflow docker-compose](https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml)

## Notes

SQL схемы:

```sql
CREATE SCHEMA ods;
CREATE SCHEMA dm;
CREATE SCHEMA stg;
```

DDL `ods.fct_earthquake`:
```sql
CREATE TABLE ods.fct_earthquake (
    time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    depth DOUBLE PRECISION NOT NULL,
    mag DOUBLE PRECISION NOT NULL,
    mag_type TEXT NOT NULL,
    nst INTEGER,
    gap DOUBLE PRECISION,
    dmin DOUBLE PRECISION,
    rms DOUBLE PRECISION,
    net TEXT NOT NULL,
    id TEXT NOT NULL,
    updated TIMESTAMPTZ NOT NULL,
    place TEXT NOT NULL,
    type TEXT NOT NULL,
    horizontal_error DOUBLE PRECISION,
    depth_error DOUBLE PRECISION,
    mag_error DOUBLE PRECISION,
    mag_nst INTEGER,
    status TEXT NOT NULL,
    location_source TEXT NOT NULL,
    mag_source TEXT NOT NULL,
    CONSTRAINT pk_fct_earthquake PRIMARY KEY (id)
);

COMMENT ON TABLE ods.fct_earthquake IS 'Таблица для хранения сырых данных о землетрясениях, загруженных из API USGS';
COMMENT ON COLUMN ods.fct_earthquake.time IS 'Время события землетрясения с часовым поясом';
COMMENT ON COLUMN ods.fct_earthquake.latitude IS 'Географическая широта в десятичных градусах';
COMMENT ON COLUMN ods.fct_earthquake.longitude IS 'Географическая долгота в десятичных градусах';
COMMENT ON COLUMN ods.fct_earthquake.depth IS 'Глубина очага в километрах';
COMMENT ON COLUMN ods.fct_earthquake.mag IS 'Магнитуда землетрясения';
COMMENT ON COLUMN ods.fct_earthquake.mag_type IS 'Тип магнитуды (например, md)';
COMMENT ON COLUMN ods.fct_earthquake.nst IS 'Количество станций, использованных для расчёта';
COMMENT ON COLUMN ods.fct_earthquake.gap IS 'Максимальный азимутальный зазор между станциями в градусах';
COMMENT ON COLUMN ods.fct_earthquake.dmin IS 'Расстояние от эпицентра до ближайшей станции в градусах';
COMMENT ON COLUMN ods.fct_earthquake.rms IS 'Корень из среднего квадрата остаточной ошибки времени';
COMMENT ON COLUMN ods.fct_earthquake.net IS 'Сеть, предоставившая данные';
COMMENT ON COLUMN ods.fct_earthquake.id IS 'Уникальный идентификатор события (первичный ключ)';
COMMENT ON COLUMN ods.fct_earthquake.updated IS 'Время последнего обновления данных';
COMMENT ON COLUMN ods.fct_earthquake.place IS 'Местоположение землетрясения в текстовом формате';
COMMENT ON COLUMN ods.fct_earthquake.type IS 'Тип события (например, earthquake)';
COMMENT ON COLUMN ods.fct_earthquake.horizontal_error IS 'Горизонтальная ошибка определения местоположения';
COMMENT ON COLUMN ods.fct_earthquake.depth_error IS 'Ошибка определения глубины';
COMMENT ON COLUMN ods.fct_earthquake.mag_error IS 'Ошибка определения магнитуды';
COMMENT ON COLUMN ods.fct_earthquake.mag_nst IS 'Количество станций для расчёта магнитуды';
COMMENT ON COLUMN ods.fct_earthquake.status IS 'Статус события (например, automatic)';
COMMENT ON COLUMN ods.fct_earthquake.location_source IS 'Источник данных о местоположении';
COMMENT ON COLUMN ods.fct_earthquake.mag_source IS 'Источник данных о магнитуде';

```

DDL `dm.fct_count_day_earthquake`:

```sql
CREATE TABLE dm.fct_count_day_earthquake AS 
SELECT time::date AS date, count(*)
FROM ods.fct_earthquake
GROUP BY 1
```

DDL `dm.fct_avg_day_earthquake`:

```sql
CREATE TABLE dm.fct_avg_day_earthquake AS
SELECT time::date AS date, avg(mag::float)
FROM ods.fct_earthquake
GROUP BY 1 
```