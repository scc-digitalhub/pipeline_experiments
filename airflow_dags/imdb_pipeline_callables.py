import os
import json
import pandas as pd
from json import JSONDecodeError
from time import sleep

from indices_imdb.downloader import Downloader
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser
from pymongo import MongoClient

from airflow.decorators import task

@task
def get_apis(api_names, data_dir):
    #available APIs
    apis = []
    for api_name in api_names:
        if api_name == "credits":
            apis.append({
                "name": "credits",
                "path": "/credits/{}",
                "content": "cast",
                #"parser": CreditsParser(work_file=os.path.join(data_dir, "config", "endpoints_cast.txt"))
                "parser_file": os.path.join(data_dir, "config", "endpoints_cast.txt")
            })
        elif api_name == "business":
            apis.append({
                "name": "business",
                "path": "/business/{}",
                "content": "resource",
                #"parser": BusinessParser(countries_file=os.path.join(data_dir, "config", "countryEU_codes.txt"))
                "parser_file": os.path.join(data_dir, "config", "countryEU_codes.txt")
            })
    return apis

@task
def get_titles_dataframe(titles_path, chunk_start, chunk_size):
    tdf = pd.read_csv(titles_path)
    #convert rows from Series obj to json String for XComs
    return [row.to_json() for index, row in tdf[chunk_start:chunk_size + chunk_start].iterrows()]

@task
def get_downloader_tasks(apis, titles, overwrite, database):
    tasks = []
    #open connection with db
    client = MongoClient(database["host"], database["port"])
    try:
        for api in apis:
            for row_json in titles:
                row = json.loads(row_json)
                tconst = row["tconst"]
                skip = False
                if not overwrite:
                    #check if title has been already downloaded
                    db = client[database["db_name"]]
                    collection_name = "download_" + api["name"]
                    res = db[collection_name].find_one(filter={"tconst": tconst}, projection=["_id"])
                    skip = res!=None
                if skip:
                    print("Skipping download of {} for {}".format(api["name"], tconst))
                else:
                    print("Adding task for {} and tconst {}".format(api["name"], tconst))
                    tasks.append((api, tconst))
    finally:
        print("Closing client connection")
        client.close()
    return tasks

@task
def download_title(download_tuple, database, nginx_host, data_dir, overwrite):
    (api, tconst) = download_tuple
    downloader = Downloader(nginx_host, api, data_dir, overwrite=overwrite, delay=0)
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
            print("Successfully downloaded {} for {}".format(downloader.api_name, tconst))
        finally:
            print("Closing client connection for {} and {}".format(downloader.api_name, tconst))
            client.close()
    except JSONDecodeError as json_err:
        print("Download of {} for {} failed: title not found".format(downloader.api_name, tconst))
    except Exception as err:
        print("Exception occurred during download of {} for {}".format(downloader.api_name, tconst))
        raise err

@task
def get_processor_tasks(apis, titles, overwrite, database):
    tasks = []
    #open connection with db
    client = MongoClient(database["host"], database["port"])
    try:
        for api in apis:
            for row_json in titles:
                #reconvert rows to Series obj
                row = pd.Series(json.loads(row_json))
                tconst = row["tconst"]
                skip = False
                if not overwrite:
                    #open connection and check if title has been already processed
                    db = client[database["db_name"]]
                    collection_name = "process_" + api["name"]
                    res = db[collection_name].find_one(filter={"tconst": tconst}, projection=["_id"])
                    skip = res!=None
                if skip:
                    print("Skipping processing of {} for {}".format(api["name"], tconst))
                else:
                    print("Adding task for {} and tconst {}".format(api["name"], tconst))
                    tasks.append((api, tconst, row_json))
    finally:
        print("Closing client connection")
        client.close()
    return tasks

@task
def process_title(process_tuple, database):
    (api, tconst, row_json) = process_tuple
    #reconvert rows to Series obj
    row = pd.Series(json.loads(row_json))
    client = MongoClient(database["host"], database["port"])
    try:
        download_coll_name = "download_" + api["name"]
        processing_coll_name = "process_" + api["name"]
        db = client[database["db_name"]]
        downloaded_data = db[download_coll_name].find_one({"tconst": tconst})
        if downloaded_data is None:
            #log error but do not stop execution
            print("Processing of {} for {} failed: title not downloaded".format(api["name"], tconst))
        else:
            if api["name"] == "credits":
                parser = CreditsParser(work_file=api["parser_file"])
            else:
                parser = BusinessParser(countries_file=api["parser_file"])
            processed_data = parser.parse_title_data(tconst, row, downloaded_data)
            db[processing_coll_name].replace_one({"tconst": tconst}, processed_data, upsert=True)
            print("Successfully processed {} for {}".format(api["name"], tconst))
    except Exception as err:
        if isinstance(err, TypeError) and err.args[0].startswith("'NoneType' object is not subscriptable"):
            print("Processing of {} for {} failed: some data is missing".format(api["name"], tconst))
        else:
            print("Exception occurred during processing of {} for {}".format(api["name"], tconst))
            raise err
    finally:
        print("Closing client connection for {} and {}".format(api["name"], tconst))
        client.close()

@task
def log_stats_from_db(apis, titles, database):
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
        print("Statistics: {}".format(stats))
    finally:
        print("Closing client connection")
        client.close()
