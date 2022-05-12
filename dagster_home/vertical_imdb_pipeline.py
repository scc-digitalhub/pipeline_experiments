import os
import pandas as pd
from json import JSONDecodeError
from time import sleep
from pymongo import MongoClient
from indices_imdb.downloader import Downloader
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser

from dagster import job, op, graph, resource, make_values_resource, get_dagster_logger, static_partitioned_config, DynamicOut, DynamicOutput, In, Out, Nothing, Field, Array

############# Resource definition
class MongoDBResource:
    def __init__(self, host: str, port: int, db: str):
        self.logger = get_dagster_logger()
        self.logger.info("Creating MongoDB connection")
        #create db connection
        self.client = MongoClient(host=host, port=port)
        #get or create Database obj
        self.db = self.client[db]

    def read(self, collection_name: str):
        #return a pymongo.cursor.Cursor
        self.logger.info("Returning cursor for: {}".format(collection_name))
        return self.db[collection_name].find()

    def count(self, collection_name: str) -> int:
        #NOTE: using collection metadata instead of counting docs one by one, result may be incorrect
        #e.g. in case of sharded clusters, unclean shutdown
        res = self.db[collection_name].estimated_document_count()
        self.logger.info("Estimated doc count for collection {} is {}".format(collection_name, res))
        return res

    def get(self, collection_name: str, tconst: str) -> dict:
        res = self.db[collection_name].find_one({"tconst": tconst})
        self.logger.info("Returning: {} for tconst {}".format(res, tconst))
        return res

    def contains(self, collection_name: str, tconst: str) -> bool:
        res = self.db[collection_name].find_one({"tconst": tconst})
        self.logger.info("Found: {}".format(res))
        return res != None

    def insert_or_replace(self, collection_name: str, tconst: str, data):
        res = self.db[collection_name].replace_one({"tconst": tconst}, data, upsert=True)
        self.logger.info("Replaced: {}, inserted: {}".format(res.modified_count, res.upserted_id))
    
    def close_connection(self):
        self.logger.info("Closing MongoDB connection")
        self.client.close()

@resource(config_schema={"host": str, "port": int, "database_name": str})
def mongodb_resource(init_context):
    #open and close a connection each time an op requires the resource
    host = init_context.resource_config["host"]
    port = init_context.resource_config["port"]
    db = init_context.resource_config["database_name"]
    client = MongoDBResource(host, port, db)
    yield client
    client.close_connection()

############# Partitions definition
CHUNK_SIZE = 15
CHUNKS = [str(x) for x in range(0,5001,CHUNK_SIZE)]

@static_partitioned_config(partition_keys=CHUNKS)
def chunk_config(partition_key: str):
    return {"ops": {"get_titles_dataframe": {"config": {"chunk_start": int(partition_key)}}}}

############# Op definition
@op(out={"apis": Out(list)}, required_resource_keys={"execution_params"})
def get_input_apis(context):
    data_dir = context.resources.execution_params["data_directory"]
    nginx_host = context.resources.execution_params["nginx_host"]
    overwrite = context.resources.execution_params["overwrite"]
    delay = context.resources.execution_params["delay"]
    apis = context.resources.execution_params["apis"]
    api_dicts = []

    for api_name in apis:
        if api_name == "credits":
            api_dict = {
                "name": "credits",
                "path": "/credits/{}",
                "content": "cast",
                "parser": CreditsParser(work_file=os.path.join(data_dir, "config", "endpoints_cast.txt"))
            }
            api_dict["downloader"] = Downloader(nginx_host, api_dict, data_dir, overwrite, delay)
            api_dicts.append(api_dict)
        elif api_name == "business":
            api_dict = {
                "name": "business",
                "path": "/business/{}",
                "content": "resource",
                "parser": BusinessParser(countries_file=os.path.join(data_dir, "config", "countryEU_codes.txt"))
            }
            api_dict["downloader"] = Downloader(nginx_host, api_dict, data_dir, overwrite, delay)
            api_dicts.append(api_dict)
    return api_dicts

@op(
    out=DynamicOut(description="Title row"),
    required_resource_keys={"execution_params"},
    config_schema={
        "chunk_start": int,
        "chunk_size": Field(int, default_value=CHUNK_SIZE, is_required=False)
    })
def get_titles_dataframe(context):
    titles_file = context.resources.execution_params["titles_file"]
    chunk_start = context.op_config["chunk_start"]
    chunk_size = context.op_config["chunk_size"]
    tdf = pd.read_csv(titles_file)
    #slice titles in the selected partition (if selected partition is out of bounds, no output is returned)
    for (index, row) in tdf[chunk_start:chunk_size + chunk_start].iterrows():
        yield DynamicOutput(
            value=row,
            mapping_key=row["tconst"]
        )

@op(
    ins={"title": In(description="Title row"), "apis": In(list)},
    out={"title_tuple": Out(tuple, description="Title row and api list")},
    required_resource_keys={"database"})
def download_title(context, title, apis):
    logger = get_dagster_logger()
    tconst = title["tconst"]
    #attemp to download title for all apis
    for api in apis:
        try:
            downloader = api["downloader"]
            collection_name = "download_" + api["name"]
            if not downloader.overwrite and context.resources.database.contains(collection_name, tconst):
                logger.info("Skipped download of {} for {}".format(api["name"], tconst))
            else:
                if(downloader.delay > 0):
                    sleep(downloader.delay)
                json_data = downloader.call_api(tconst)
                if "content" in api and not api["content"] in json_data:
                    #log error but do not stop execution
                    logger.info("Skipped storing {} for {}: key {} is missing".format(api["name"], tconst, downloader.api_content))
                else:
                    json_data["tconst"] = tconst #add it as identifier for any API
                    context.resources.database.insert_or_replace(collection_name, tconst, json_data)
                    logger.info("Successfully downloaded {} for {}".format(api["name"], tconst))
        except JSONDecodeError as json_err:
            #log error but do not stop execution
            logger.error("Download of {} for {} failed: title not found".format(api["name"], tconst))
        except Exception as err:
            logger.error("Exception occurred during download of {} for {}".format(api["name"], tconst))
            raise err
    return (title, apis)

@op(ins={"title_tuple": In(tuple, description="Title row and api list")}, required_resource_keys={"database"})
def process_title(context, title_tuple):
    logger = get_dagster_logger()
    (title, apis) = title_tuple
    tconst = title["tconst"]
    #attemp to process title for all apis
    for api in apis:
        try:
            download_coll_name = "download_" + api["name"]
            processing_coll_name = "process_" + api["name"]
            if not api["downloader"].overwrite and context.resources.database.contains(processing_coll_name, tconst):
                logger.info("Skipped processing of {} for {}".format(api["name"], tconst))
            else:
                downloaded_data = context.resources.database.get(download_coll_name, tconst)
                if downloaded_data is None:
                    #log error but do not stop execution
                    logger.error("Processing of {} for {} failed: title not downloaded".format(api["name"], tconst))
                else:
                    processed_data = api["parser"].parse_title_data(tconst, title, downloaded_data)
                    context.resources.database.insert_or_replace(processing_coll_name, tconst, processed_data)
                    logger.info("Successfully processed {} for {}".format(api["name"], tconst))
        except Exception as err:
            if isinstance(err, TypeError) and err.args[0].startswith("'NoneType' object is not subscriptable"):
                #log error but do not stop execution
                logger.error("Processing of {} for {} failed: some data is missing".format(api["name"], tconst))
            else:
                logger.error("Exception occurred during processing of {} for {}".format(api["name"], tconst))
                raise err

@graph
def title_graph(title, apis):
    return process_title(download_title(title, apis))

@op(ins={"upstream": In(Nothing), "apis": In(list), "titles": In(list)}, required_resource_keys={"database"})
def log_stats_from_db(context, apis, titles):
    logger = get_dagster_logger()
    stats = {}
    for api in apis:
        api_stats = {
            "titles": len(titles),
            "downloaded": context.resources.database.count("download_" + api["name"]),
            "processed": context.resources.database.count("process_" + api["name"])
        }
        stats[api["name"]] = api_stats
    logger.info("Statistics: {}".format(stats))

############# Job definition
@job(
    config=chunk_config,
    resource_defs={
        "database": mongodb_resource,
        "execution_params": make_values_resource(apis=Array(str), data_directory=str, titles_file=str, nginx_host=str, overwrite=bool, delay=int)
    })
def vertical_imdb_pipeline():
    apis = get_input_apis()
    titles = get_titles_dataframe()

    result = titles.map(lambda title: title_graph(title, apis))

    #TODO keep one collection for each partition to track downl and proc for each partition or unique count?
    log_stats_from_db(apis, titles.collect(), upstream=result.collect())

'''
resources:
  database:
    config:
      host: "localhost"
      port: 27017
      database_name: "dagster_imdb2"
  execution_params:
    config:
      apis: ["credits", "fooo", "business"]
      data_directory: "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp"
      titles_file: "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp/titles.csv"
      nginx_host: "localhost"
      overwrite: True
      delay: 0
'''