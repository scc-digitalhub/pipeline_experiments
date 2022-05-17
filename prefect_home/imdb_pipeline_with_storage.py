# IMDb pipeline that connects to MongoDB to persist data
# NOTE: To parallelize DB operations with mapping, mapped tasks must be completely self-contained, so
# a resource_manager is not suitable for this scenario (see https://www.prefect.io/blog/orchestrating-elt-with-prefect-dbt-cloud-and-snowflake-part-3/)
import os
from time import sleep
from json import JSONDecodeError
import pandas as pd
from indices_imdb.downloader import Downloader
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser
from pymongo import MongoClient

import prefect
from prefect import task, Flow, Parameter, unmapped, flatten
from prefect.engine import signals
from prefect.executors import LocalDaskExecutor
from prefect.triggers import any_successful
from prefect.tasks.control_flow.filter import FilterTask

#excludes wrong APIs
filter_apis = FilterTask(
    filter_func=lambda x: not isinstance(x, type(None))
)

@task
def get_api(api_name, data_dir):
    #available APIs
    if api_name == "credits":
        return {
            "name": "credits",
            "path": "/credits/{}",
            "content": "cast",
            "parser": CreditsParser(work_file=os.path.join(data_dir, "config", "endpoints_cast.txt"))
        }
    elif api_name == "business":
        return {
            "name": "business",
            "path": "/business/{}",
            "content": "resource",
            "parser": BusinessParser(countries_file=os.path.join(data_dir, "config", "countryEU_codes.txt"))
        }
    else:
        return None

@task
def get_titles_dataframe(titles_path, chunk_start, chunk_size):
    tdf = pd.read_csv(titles_path)
    return [row for index, row in tdf[chunk_start:chunk_size + chunk_start].iterrows()]

@task
def get_downloader_tasks(api, titles, nginx_host, overwrite, database):
    logger = prefect.context.get("logger")
    logger.info("Creating download tasks for api " + api["name"])
    downloader = Downloader(nginx_host, api, overwrite=overwrite, delay=0)
    tasks = []
    #open connection with db
    client = MongoClient(database["host"], database["port"])
    try:
        for row in titles:
            tconst = row["tconst"]
            skip = False
            if not overwrite:
                #check if title has been already downloaded
                db = client[database["db_name"]]
                collection_name = "download_" + downloader.api_name
                res = db[collection_name].find_one(filter={"tconst": tconst}, projection=["_id"])
                skip = res!=None
            if skip:
                logger.info("Skipping download of {} for {}".format(downloader.api_name, tconst))
            else:
                logger.info("Adding task for {} and tconst {}".format(downloader.api_name, tconst))
                tasks.append((downloader, tconst))
    finally:
        logger.info("Closing client connection for {}".format(downloader.api_name))
        client.close()
    return tasks

@task
def download_title(download_tuple, database):
    logger = prefect.context.get("logger")
    (downloader, tconst) = download_tuple
    try:
        if(downloader.delay > 0):
            sleep(downloader.delay)
        json_data = downloader.call_api(tconst)
        if downloader.api_content and not downloader.api_content in json_data:
            raise Exception("Missing key {} for {}, skip.".format(downloader.api_content, tconst))
        json_data["tconst"] = tconst #add it as identifier for any API
        client = MongoClient(database["host"], database["port"])
        try:
            db = client[database["db_name"]]
            collection_name = "download_" + downloader.api_name
            db[collection_name].replace_one({"tconst": tconst}, json_data, upsert=True)
            logger.info("Successfully downloaded {} for {}".format(downloader.api_name, tconst))
            #return downloader.api_name
        finally:
            logger.info("Closing client connection for {} and {}".format(downloader.api_name, tconst))
            client.close()
    except JSONDecodeError as json_err:
        logger.error("Download of {} for {} failed: title not found".format(downloader.api_name, tconst))
        raise signals.FAIL(json_err)
    except Exception as err:
        logger.error("Exception occurred during download of {} for {}".format(downloader.api_name, tconst))
        raise err

@task(trigger=any_successful)
def get_processor_tasks(api, titles, overwrite, database):
    logger = prefect.context.get("logger")
    logger.info("Creating processor tasks for api " + api["name"])
    tasks = []
    #open connection with db
    client = MongoClient(database["host"], database["port"])
    try:
        for row in titles:
            tconst = row["tconst"]
            skip = False
            if not overwrite:
                #open connection and check if title has been already processed
                db = client[database["db_name"]]
                collection_name = "process_" + api["name"]
                res = db[collection_name].find_one(filter={"tconst": tconst}, projection=["_id"])
                skip = res!=None
            if skip:
                logger.info("Skipping processing of {} for {}".format(api["name"], tconst))
            else:
                logger.info("Adding task for {} and tconst {}".format(api["name"], tconst))
                tasks.append((api, tconst, row))
    finally:
        logger.info("Closing client connection for {}".format(api["name"]))
        client.close()
    return tasks

@task
def process_title(process_tuple, database):
    logger = prefect.context.get("logger")
    (api, tconst, row) = process_tuple
    client = MongoClient(database["host"], database["port"])
    try:
        download_coll_name = "download_" + api["name"]
        processing_coll_name = "process_" + api["name"]
        db = client[database["db_name"]]
        downloaded_data = db[download_coll_name].find_one({"tconst": tconst})
        if downloaded_data is None:
            #log error but do not stop execution
            logger.error("Processing of {} for {} failed: title not downloaded".format(api["name"], tconst))
        else:
            processed_data = api["parser"].parse_title_data(tconst, row, downloaded_data)
            db[processing_coll_name].replace_one({"tconst": tconst}, processed_data, upsert=True)
            logger.info("Successfully processed {} for {}".format(api["name"], tconst))
            #return api["name"]
    except Exception as err:
        if isinstance(err, TypeError) and err.args[0].startswith("'NoneType' object is not subscriptable"):
            logger.error("Processing of {} for {} failed: some data is missing".format(api["name"], tconst))
            raise signals.FAIL(err.args[0])
        else:
            logger.error("Exception occurred during processing of {} for {}".format(api["name"], tconst))
            raise err
    finally:
        logger.info("Closing client connection for {} and {}".format(api["name"], tconst))
        client.close()

@task(trigger=any_successful)
def log_stats_from_db(apis, titles, database):
    logger = prefect.context.get("logger")
    stats = {}
    client = MongoClient(database["host"], database["port"])
    try:
        db = client[database["db_name"]]
        for api in apis:
            #NOTE: using collection metadata instead of counting docs one by one, result may be incorrect
            #e.g. in case of sharded clusters, unclean shutdown
            api_stats = {
                "titles": len(titles),
                "downloaded": db["download_" + api["name"]].estimated_document_count(),
                "processed": db["process_" + api["name"]].estimated_document_count()
            }
            stats[api["name"]] = api_stats
        logger.info("Statistics: {}".format(stats))
    finally:
        logger.info("Closing client connection")
        client.close()

with Flow("IMDb Data Flow with MongoDB") as flow:
    input_apis = Parameter("apis", default=[])
    titles_file = Parameter("titles_file")
    data_directory = Parameter("data_directory")
    nginx_host = Parameter("nginx_host")
    overwrite = Parameter("overwrite")
    database = Parameter("database")
    chunk_start = Parameter("chunk_start", default=0)
    chunk_size = Parameter("chunk_size", default=15)

    apis = filter_apis(get_api.map(input_apis, unmapped(data_directory)))
    titles = get_titles_dataframe(titles_file, chunk_start, chunk_size)

    downloader_tasks = get_downloader_tasks.map(apis, unmapped(titles), unmapped(nginx_host), unmapped(overwrite), unmapped(database))
    downloader_results = download_title.map(flatten(downloader_tasks), unmapped(database))

    processor_tasks = get_processor_tasks.map(apis, unmapped(titles), unmapped(overwrite), unmapped(database))
    processor_tasks.set_upstream(downloader_results)
    processor_results = process_title.map(flatten(processor_tasks), unmapped(database))

    log_stats_from_db(apis, titles, database).set_upstream(processor_results)

if __name__=="__main__":
    flow.run(
        executor=LocalDaskExecutor(),
        parameters={
            "apis": ["credits", "fooo", "business"],
            "data_directory": "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp",
            "titles_file": "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp/titles.csv",
            "nginx_host": "localhost",
            "overwrite": False,
            "database": {"host": "localhost", "port": 27017, "db_name": "prefect_imdb"},
            "chunk_start": 0
        }
    )
    flow.visualize()