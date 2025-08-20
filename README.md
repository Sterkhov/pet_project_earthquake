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
CREATE TABLE ods.fct_earthquake
(
	time varchar,
	latitude varchar,
	longitude varchar,
	depth varchar,
	mag varchar,
	mag_type varchar,
	nst varchar,
	gap varchar,
	dmin varchar,
	rms varchar,
	net varchar,
	id varchar,
	updated varchar,
	place varchar,
	type varchar,
	horizontal_error varchar,
	depth_error varchar,
	mag_error varchar,
	mag_nst varchar,
	status varchar,
	location_source varchar,
	mag_source varchar
)
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