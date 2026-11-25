from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from src.pipeline import consolidate, ingest

BRONZE = Dataset("sqlite:///data/observatorio.db/raw")
defaults = dict(retries=2, retry_delay=timedelta(minutes=2), retry_exponential_backoff=True)


def extract(dag_run, data_interval_start, data_interval_end, **context):
    conf = dag_run.conf or {}
    start = conf.get("start", data_interval_start.to_date_string())
    end = conf.get("end", data_interval_end.to_date_string())
    ingest(start, end)


with DAG("noticias_ingestao", schedule="@daily", catchup=False,
         start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), max_active_runs=1,
         default_args=defaults, is_paused_upon_creation=False) as raw_dag:
    PythonOperator(task_id="coletar_paginas", python_callable=extract, outlets=[BRONZE])

with DAG("noticias_consolidacao", schedule=[BRONZE], catchup=False,
         start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), max_active_runs=1,
         default_args=defaults, is_paused_upon_creation=False) as dw_dag:
    PythonOperator(task_id="carregar_estrela", python_callable=consolidate)
