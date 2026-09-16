FROM apache/airflow:2.9.3-python3.11

USER root

WORKDIR /opt/airflow

COPY pyproject.toml weather_etl.py ./
COPY dags ./dags

RUN chown -R airflow:0 /opt/airflow

USER airflow

RUN pip install --no-cache-dir -e .
