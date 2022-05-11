import os
import pandas as pd
from json import JSONDecodeError
from time import sleep
from pymongo import MongoClient
from indices_imdb.downloader import Downloader
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser

from dagster import job, op, resource, make_values_resource, get_dagster_logger, DynamicOut, DynamicOutput, In, Out, Output, Nothing

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

############# Op definition
@op(required_resource_keys={"execution_params"})
def get_input_apis(context):
    data_dir = context.resources.execution_params["data_directory"]
    apis = context.resources.execution_params["apis"]
    api_dicts = []

    for api_name in apis:
        if api_name == "credits":
            api_dicts.append({
                "name": "credits",
                "path": "/credits/{}",
                "content": "cast",
                "parser": CreditsParser(work_file=os.path.join(data_dir, "config", "endpoints_cast.txt"))
            })
        elif api_name == "business":
            api_dicts.append({
                "name": "business",
                "path": "/business/{}",
                "content": "resource",
                "parser": BusinessParser(countries_file=os.path.join(data_dir, "config", "countryEU_codes.txt"))
            })
    return api_dicts

@op(required_resource_keys={"execution_params"})
def get_titles_dataframe(context):
    titles_file = context.resources.execution_params["titles_file"]
    tdf = pd.read_csv(titles_file)
    return [(index, row) for index, row in tdf.iterrows()]

@op(out=DynamicOut(), required_resource_keys={"execution_params"})
def get_downloader_tasks(context, apis, titles):
    nginx_host = context.resources.execution_params["nginx_host"]
    data_dir = context.resources.execution_params["data_directory"]
    overwrite = context.resources.execution_params["overwrite"]
    for api in apis:
        downloader = Downloader(nginx_host, api, data_dir, overwrite, delay=0)
        for (index, row) in titles:
            yield DynamicOutput(
                value=(downloader, row["tconst"]),
                mapping_key="download_{}_{}".format(api["name"], row["tconst"])
            )

@op(out=Out(is_required=False), required_resource_keys={"database"})
def download_title(context, download_tuple):
    logger = get_dagster_logger()
    (downloader, tconst) = download_tuple
    try:
        collection_name = "download_" + downloader.api_name
        if not downloader.overwrite and context.resources.database.contains(collection_name, tconst):
            logger.info("Skipped download of {} for {}".format(downloader.api_name, tconst))
        else:
            if(downloader.delay > 0):
                sleep(downloader.delay)
            json_data = downloader.call_api(tconst)
            if downloader.api_content:
                if not downloader.api_content in json_data:
                    raise Exception("Missing key {} for {}, skip.".format(downloader.api_content, tconst))
            json_data["tconst"] = tconst #add it as identifier for any API
            context.resources.database.insert_or_replace(collection_name, tconst, json_data)
            logger.info("Successfully downloaded {} for {}".format(downloader.api_name, tconst))

        yield Output(downloader.api_name)
    except JSONDecodeError as json_err:
        #log error but do not stop execution
        logger.error("Download of {} for {} failed: title not found".format(downloader.api_name, tconst))
    except Exception as err:
        logger.error("Exception occurred during download of {} for {}".format(downloader.api_name, tconst))
        raise err

@op(ins={"upstream": In(Nothing)}, out=DynamicOut())
def get_processor_tasks(apis, titles):
    for api in apis:
        for (index, row) in titles:
            yield DynamicOutput(
                value=(api, row["tconst"], row),
                mapping_key="process_{}_{}".format(api["name"], row["tconst"])
            )

@op(out=Out(is_required=False), required_resource_keys={"execution_params", "database"})
def process_title(context, process_tuple):
    logger = get_dagster_logger()
    overwrite = context.resources.execution_params["overwrite"]
    (api, tconst, row) = process_tuple
    try:
        download_coll_name = "download_" + api["name"]
        processing_coll_name = "process_" + api["name"]
        if not overwrite and context.resources.database.contains(processing_coll_name, tconst):
            logger.info("Skipped processing of {} for {}".format(api["name"], tconst))
        else:
            downloaded_data = context.resources.database.get(download_coll_name, tconst)
            if downloaded_data is None:
                #log error but do not stop execution
                logger.error("Processing of {} for {} failed: title not downloaded".format(api["name"], tconst))
            else:
                if "content" in api:
                    if not api["content"] in downloaded_data:
                        #TODO same check happens at download, remove here?
                        raise Exception("missing resource for {}, skip.".format(tconst))
                processed_data = api["parser"].parse_title_data(tconst, row, downloaded_data)
                context.resources.database.insert_or_replace(processing_coll_name, tconst, processed_data)
                logger.info("Successfully processed {} for {}".format(api["name"], tconst))

                #TODO return nothing?
                yield Output(api["name"])
    except Exception as err:
        if isinstance(err, TypeError) and err.args[0].startswith("'NoneType' object is not subscriptable"):
            #log error but do not stop execution
            logger.error("Processing of {} for {} failed: some data is missing".format(api["name"], tconst))
        else:
            logger.error("Exception occurred during processing of {} for {}".format(api["name"], tconst))
            raise err

# @op(required_resource_keys={"execution_params", "database"})
# def create_datasets(context, processor_results):
#     data_dir = context.resources.execution_params["data_directory"]
#     distinct_apis = set(processor_results)
#     #db is expected to contain a process collection for each api
#     for api_name in distinct_apis:
#         ll = []
#         collection_name = "process_" + api_name
#         #get all documents
#         for processed_data in context.resources.database.read(collection_name):
#             ll.append(pd.DataFrame([processed_data]))
#         #write documents to file
#         df = pd.concat(ll)
#         outpath = os.path.join(data_dir, api_name + ".csv")
#         df.to_csv(outpath)

@op(ins={"upstream_down": In(Nothing), "upstream_proc": In(Nothing)}, required_resource_keys={"database"})
def log_stats_from_db(context, apis, titles):
    #takes 4 seconds to run
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

@job(resource_defs={"database": mongodb_resource, "execution_params": make_values_resource()})
def imdb_pipeline_with_storage():
    apis = get_input_apis()
    titles = get_titles_dataframe()

    to_be_downloaded = get_downloader_tasks(apis, titles)
    downloader_results = to_be_downloaded.map(download_title)

    to_be_processed = get_processor_tasks(apis, titles, upstream=downloader_results.collect())
    processor_results = to_be_processed.map(process_title)

    #create_datasets(processor_results.collect())

    log_stats_from_db(apis, titles, upstream_down=downloader_results.collect(), upstream_proc=processor_results.collect())


'''
resources:
  database:
    config:
      host: "localhost"
      port: 27017
      database_name: "dagster_imdb"
  execution_params:
    config:
      apis: ["credits", "fooo", "business"]
      data_directory: "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp"
      titles_file: "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp/titles.csv"
      nginx_host: "localhost"
      overwrite: True
'''


# client = MongoClient(host="localhost", port=27017) #create db connection
# db = client.dagster_imdb #Database obj, se il db non esiste viene creato
# my_collection = db.my_collection #Collection obj
# my_document = {"name": "John", "Surname": "Doe"}
# my_collection.insert_one(my_document) #returns InsertOneResult obj with given _id

# for doc in my_collection.find(): #returns Cursor obj
#     print(doc)

# john_doc = my_collection.find_one({"name": "John"})

# some_coll = db.get_collection("coll_name")
# collections_list = db.list_collection_names()
# db.drop_collection("coll_name")
# client.close() #keep connection alive while working and only close it before exiting the application