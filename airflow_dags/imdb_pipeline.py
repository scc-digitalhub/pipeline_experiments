from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

import json
import os
from imdb_pipeline_callables import get_apis, get_titles_dataframe, get_downloader_tasks, download_title, get_processor_tasks, process_title, log_stats_from_db

# IMDb Airflow pipeline that connects to MongoDB to persist data
@dag(
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False
)
def imdb_pipeline():
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    dag_config_path = os.path.join(dag_dir, 'imdb_pipeline_config.json')
    with open(dag_config_path) as dag_config:
        configuration = json.load(dag_config)
    
    apis = get_apis(configuration['apis'], configuration['data_directory'])
    titles = get_titles_dataframe(configuration['titles_file'], configuration['chunk_start'], configuration['chunk_size'])

    to_be_downloaded = get_downloader_tasks(apis, titles, configuration['overwrite'], configuration['database'])
    downloader_results = download_title.partial(database=configuration['database'], nginx_host=configuration['nginx_host'], data_dir=configuration['data_directory'], overwrite=configuration['overwrite']).expand(download_tuple=to_be_downloaded)

    to_be_processed = get_processor_tasks(apis, titles, configuration['overwrite'], configuration['database'])
    downloader_results >> to_be_processed
    processor_results = process_title.partial(database=configuration['database']).expand(process_tuple=to_be_processed)

    stats = log_stats_from_db(apis, titles, configuration['database'])
    processor_results >> stats

imdb_pipeline_dag = imdb_pipeline()
