from json import JSONDecodeError
import os
import pandas as pd
from indices_imdb.downloader import Downloader
from indices_imdb.processor import Processor
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser

from dagster import job, op, make_values_resource, get_dagster_logger, DynamicOut, DynamicOutput, In, Out, Output, Nothing

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

@op(out=Out(is_required=False))
def download_title(download_tuple):
    logger = get_dagster_logger()
    (downloader, tconst) = download_tuple
    try:
        downloader.download_title(tconst)
        logger.info("Successfully downloaded {} for {}".format(downloader.api_name, tconst))
        yield Output(downloader.api_name)
    except JSONDecodeError as json_err:
        #log error but do not stop execution
        logger.error("Download of {} for {} failed: title not found".format(downloader.api_name, tconst))
    except Exception as err:
        logger.error("Exception occurred during download of {} for {}".format(downloader.api_name, tconst))
        raise err

@op
def get_chunk_index():
    return 0

@op(ins={"upstream": In(Nothing)}, out=DynamicOut(), required_resource_keys={"execution_params"})
def get_processor_tasks(context, apis, titles, chunk_index):
    logger = get_dagster_logger()
    data_dir = context.resources.execution_params["data_directory"]
    overwrite = context.resources.execution_params["overwrite"]
    for api in apis:
        basedir = os.path.join(data_dir, "process", api["name"])
        if not os.path.exists(basedir):
            os.makedirs(basedir)
        batch_outpath = os.path.join(basedir, "{}_{}.parquet".format(api["name"], chunk_index))
        if not overwrite and os.path.exists(batch_outpath):
            #skip processing for this batch
            logger.info("Batch {} for chunk {} already exists".format(api["name"], chunk_index))
        else:
            processor = Processor(api, api["parser"], data_dir)
            for (index, row) in titles:
                yield DynamicOutput(
                    value=(processor, row["tconst"], row, batch_outpath),
                    mapping_key="process_{}_{}".format(api["name"], row["tconst"])
                )

@op(out=Out(is_required=False))
def process_title(process_tuple):
    logger = get_dagster_logger()
    (processor, tconst, row, batch_outpath) = process_tuple
    try:
        data = processor.process_title(tconst, row)
        logger.info("Successfully processed {} for {}".format(processor.api_name, tconst))
        title_dataframe = pd.DataFrame([data])
        yield Output((batch_outpath, title_dataframe, processor.api_name))
    except Exception as err:
        if isinstance(err.args[0], str) and err.args[0].startswith("missing data dir"):
            #log error but do not stop execution
            logger.error("Processing of {} for {} failed: title not downloaded".format(processor.api_name, tconst))
        elif isinstance(err, TypeError) and err.args[0].startswith("'NoneType' object is not subscriptable"):
            #log error but do not stop execution
            logger.error("Processing of {} for {} failed: some data is missing".format(processor.api_name, tconst))
        else:
            logger.error("Exception occurred during processing of {} for {}".format(processor.api_name, tconst))
            raise err

@op(out=DynamicOut())
def group_titles_by_batch(processor_results):
    batches_per_api = {}
    for batch_outpath, title_dataframe, api_name in processor_results:
        if not api_name in batches_per_api:
            batches_per_api[api_name] = {}
        if not batch_outpath in batches_per_api[api_name]:
            batches_per_api[api_name][batch_outpath] = []
        batches_per_api[api_name][batch_outpath].append(title_dataframe)
    
    # batches_per_api = {
    #     "credits": {
    #         "credits_0.parquet": [df1,df2,df3],
    #         "credits_1.parquet": [df4,df5,df6]
    #     },
    #     "business": {
    #         "business_0.parquet": []
    #     }
    # }
    for api, batches_dict in batches_per_api.items():
        for batch_file, dataframes in batches_dict.items():
            yield DynamicOutput(
                value=(api, batch_file, dataframes),
                mapping_key=os.path.basename(batch_file).split(".")[0]
            )

@op
def create_batch_file(batch_tuple):
    (api_name, batch_file, dataframes) = batch_tuple
    bdf = pd.concat(dataframes)
    bdf.to_parquet(batch_file)
    return (api_name, batch_file)

@op(out=DynamicOut())
def group_batches_by_api(batch_files):
    files_per_api = {}
    for (api_name, batch_file) in batch_files:
        if not api_name in files_per_api:
            files_per_api[api_name] = []
        files_per_api[api_name].append(batch_file)
    for (api, files) in files_per_api.items():
        yield DynamicOutput(
            value=(api, files),
            mapping_key=api
        )

@op(required_resource_keys={"execution_params"})
def create_dataset(context, api_tuple):
    data_dir = context.resources.execution_params["data_directory"]
    (api_name, batch_files) = api_tuple
    ll = []
    for file in batch_files:
        ldf = pd.read_parquet(file)
        ll.append(ldf)
    df = pd.concat(ll)
    outpath = os.path.join(data_dir, "process", api_name, api_name + ".csv")
    df.to_csv(outpath)

@op
def log_stats(apis, titles, downloader_results, processor_results):
    logger = get_dagster_logger()
    stats = {}
    for api in apis:
        api_stats = {
            "titles": len(titles),
            "downloaded": len([api_name for api_name in downloader_results if api_name == api["name"]]),
            "processed": len([api_name for (batch_file, title_df, api_name) in processor_results if api_name == api["name"]])
        }
        stats[api["name"]] = api_stats
    logger.info("Statistics: {}".format(stats))

@job(resource_defs={"execution_params": make_values_resource()})
def imdb_pipeline():
    apis = get_input_apis()
    titles = get_titles_dataframe()

    to_be_downloaded = get_downloader_tasks(apis, titles)
    downloader_results = to_be_downloaded.map(download_title)

    chunk_index = get_chunk_index()
    to_be_processed = get_processor_tasks(apis, titles, chunk_index, upstream=downloader_results.collect())
    processor_results = to_be_processed.map(process_title)

    batches = group_titles_by_batch(processor_results.collect())
    batch_files = batches.map(create_batch_file)
    batches_by_api = group_batches_by_api(batch_files.collect())
    batches_by_api.map(create_dataset)

    log_stats(apis, titles, downloader_results.collect(), processor_results.collect())

'''
resources:
  execution_params:
    config:
      apis: ["credits", "fooo", "business"]
      data_directory: "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp"
      titles_file: "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp/titles.csv"
      nginx_host: "localhost"
      overwrite: True
'''

#execution as python script: python imdb_pipeline.py
# if __name__ == "__main__":
#     result = imdb_pipeline.execute_in_process(
#         run_config={
#             "resources": {
#                 "execution_params": {
#                     "config": {
#                         "apis": ["credits", "fooo", "business"],
#                         "data_directory": "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp",
#                         "titles_file": "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp/titles.csv",
#                         "nginx_host": "localhost",
#                         "overwrite": False
#                     }
#                 }
#             }
#         }
#     )