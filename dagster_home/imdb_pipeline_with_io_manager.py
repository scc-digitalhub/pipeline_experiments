import os
import pandas as pd
from json import JSONDecodeError
from time import sleep
from pymongo import MongoClient
from indices_imdb.downloader import Downloader
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser

from dagster import (
    job, op, graph, make_values_resource, get_dagster_logger, static_partitioned_config, io_manager, fs_io_manager,
    DynamicOut, DynamicOutput, In, Out, Nothing, Field, Array, IOManager
)
from dagster_dask import dask_executor

############# IO Manager definition
class MongoDBIOManager(IOManager):
    def __init__(self, host: str, port: int, db: str, overwrite: bool):
        self.logger = get_dagster_logger()
        self.logger.info("Creating MongoDB connection")
        #create db connection
        self.client = MongoClient(host=host, port=port)
        #get or create Database obj
        self.db = self.client[db]
        self.overwrite = overwrite

    def handle_output(self, context, obj):
        self.logger.info("context: {} {} {} {} {}".format(context.step_key, context.name, context.metadata, context.mapping_key, context.step_context))
        #insert in Mongo (handle overwrite)
        collection_prefix = "download_" if context.name == "downloaded_data" else "process_"
        #obj = {credits: {tconst: "", ...}, business: {tconst: "", ...}}
        for key, value in obj.items():
            collection_name = collection_prefix + key
            exists = self.db[collection_name].find_one({"tconst": value["tconst"]}) != None
            #if not exists -> insert_one
            #if exists and overwrite -> replace_one
            #if exists and not overwrite -> nothing
            if not exists or (exists and self.overwrite):
                res = self.db[collection_name].replace_one({"tconst": value["tconst"]}, value, upsert=True)
                self.logger.info("Replaced: {}, inserted: {} in {}".format(res.modified_count, res.upserted_id, collection_name))

    def load_input(self, context):
        #get from Mongo
        collection_prefix = "download_" if context.upstream_output.name == "downloaded_data" else "process_"
        obj = {}
        collections = [name for name in self.db.list_collection_names() if name.startswith(collection_prefix)]
        tconst = context.upstream_output.step_key.partition("[")[2].partition("]")[0] #empty if [ and ] are not in step_key
        for coll in collections:
            res = self.db[coll].find_one({"tconst": tconst})
            if res != None:
                obj[coll.split(collection_prefix)[1]] = res
        self.logger.info("obj: {}".format(obj))
        return obj

    def close_connection(self):
        self.logger.info("Closing MongoDB connection")
        self.client.close()

@io_manager(config_schema={"host": str, "port": int, "database_name": str, "overwrite": bool})
def mongodb_io_manager(init_context):
    host = init_context.resource_config["host"]
    port = init_context.resource_config["port"]
    db = init_context.resource_config["database_name"]
    overwrite = init_context.resource_config["overwrite"]
    client = MongoDBIOManager(host, port, db, overwrite)
    yield client
    client.close_connection()

############# Partitions definition
CHUNK_SIZE = 15
CHUNKS = [str(x) for x in range(0,5001,CHUNK_SIZE)]

@static_partitioned_config(partition_keys=CHUNKS)
def chunk_config(partition_key: str):
    return {"ops": {"get_titles_dataframe": {"config": {"chunk_start": int(partition_key)}}}}

############# Op definition
@op(out={"apis": Out(list, io_manager_key="fs_io")}, required_resource_keys={"execution_params"})
def get_input_apis(context):
    data_dir = context.resources.execution_params["data_directory"]
    nginx_host = context.resources.execution_params["nginx_host"]
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
            api_dict["downloader"] = Downloader(nginx_host, api_dict, data_dir, delay=delay)
            api_dicts.append(api_dict)
        elif api_name == "business":
            api_dict = {
                "name": "business",
                "path": "/business/{}",
                "content": "resource",
                "parser": BusinessParser(countries_file=os.path.join(data_dir, "config", "countryEU_codes.txt"))
            }
            api_dict["downloader"] = Downloader(nginx_host, api_dict, data_dir, delay=delay)
            api_dicts.append(api_dict)
    return api_dicts

@op(
    out=DynamicOut(description="Title row", io_manager_key="fs_io"),
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
    out={
        "title_tuple": Out(tuple, description="Title row and api list", io_manager_key="fs_io"),
        "downloaded_data": Out(dict, description="Downloaded data", io_manager_key="storage_io")
    }
)
def download_title(title, apis):
    logger = get_dagster_logger()
    tconst = title["tconst"]
    downloaded_data = {}
    #attemp to download title for all apis
    for api in apis:
        try:
            downloader = api["downloader"]
            if(downloader.delay > 0):
                sleep(downloader.delay)
            json_data = downloader.call_api(tconst)
            if "content" in api and not api["content"] in json_data:
                #log error but do not stop execution
                logger.info("Key {} is missing in {} for {}".format(downloader.api_content, api["name"], tconst))
            else:
                json_data["tconst"] = tconst #add it as identifier for any API
                downloaded_data[api["name"]] = json_data
                logger.info("Successfully downloaded {} for {}".format(api["name"], tconst))
        except JSONDecodeError as json_err:
            #log error but do not stop execution
            logger.error("Download of {} for {} failed: title not found".format(api["name"], tconst))
        except Exception as err:
            logger.error("Exception occurred during download of {} for {}".format(api["name"], tconst))
            raise err
    return (title, apis), downloaded_data

@op(ins={
        "title_tuple": In(tuple, description="Title row and api list"),
        "downloaded_data": In(dict, description="Downloaded data")
    },
    out={
        "processed_data": Out(dict, description="Processed data", io_manager_key="storage_io")
    })
def process_title(title_tuple, downloaded_data):
    logger = get_dagster_logger()
    (title, apis) = title_tuple
    tconst = title["tconst"]
    processed_data = {}
    #attemp to process title for all apis
    for api in apis:
        try:
            if not api["name"] in downloaded_data:
                #log error but do not stop execution
                logger.error("Processing of {} for {} failed: title not downloaded".format(api["name"], tconst))
            else:
                parsed_data = api["parser"].parse_title_data(tconst, title, downloaded_data[api["name"]])
                processed_data[api["name"]] = parsed_data
                logger.info("Successfully processed {} for {}".format(api["name"], tconst))
        except Exception as err:
            if isinstance(err, TypeError) and err.args[0].startswith("'NoneType' object is not subscriptable"):
                #log error but do not stop execution
                logger.error("Processing of {} for {} failed: some data is missing".format(api["name"], tconst))
            else:
                logger.error("Exception occurred during processing of {} for {}".format(api["name"], tconst))
                raise err
    return processed_data

@graph
def title_graph(title, apis):
    (title_tuple, downloaded_data) = download_title(title, apis)
    return process_title(title_tuple, downloaded_data)

############# Job definition
@job(
    config=chunk_config,
    #executor_def=dask_executor,
    resource_defs={
        "fs_io": fs_io_manager,
        "storage_io": mongodb_io_manager,
        "execution_params": make_values_resource(apis=Array(str), data_directory=str, titles_file=str, nginx_host=str, delay=int)
    })
def imdb_pipeline_with_io_manager():
    apis = get_input_apis()
    titles = get_titles_dataframe()

    result = titles.map(lambda title: title_graph(title, apis))

    #TODO keep one collection for each partition to track downl and proc for each partition or unique count?
    #log_stats_from_db(apis, titles.collect(), upstream=result.collect())

'''
resources:
  execution_params:
    config:
      apis: ["credits", "fooo", "business"]
      data_directory: "/Users/erica/pipeline_experiments/tmp"
      titles_file: "/Users/erica/pipeline_experiments/tmp/titles.csv"
      nginx_host: "localhost:8080"
      delay: 0
  storage_io:
    config:
      host: "localhost"
      port: 27017
      database_name: "dagster_imdb3"
      overwrite: True
'''