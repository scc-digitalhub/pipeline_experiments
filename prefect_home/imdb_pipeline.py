# IMDb data pipeline that used mainly functional API
# The configuration parameters are passed directly at runtime
from json import JSONDecodeError
import os
import pandas as pd
from indices_imdb.downloader import Downloader
from indices_imdb.processor import Processor
from indices_imdb.businessparser import BusinessParser
from indices_imdb.creditsparser import CreditsParser

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

#excludes failed tasks from results
filter_failed_tasks = FilterTask(
    filter_func=lambda x: not isinstance(x, BaseException)
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
def get_titles_dataframe(titles_path):
    tdf = pd.read_csv(titles_path)
    return [(index, row) for index, row in tdf.iterrows()]

@task
def get_downloader_tasks(api, titles, data_dir, nginx_host, overwrite):
    logger = prefect.context.get("logger")
    logger.info("Creating download task for api " + api["name"])
    downloader = Downloader(nginx_host, api, data_dir, overwrite, delay=0)
    return [(downloader, row["tconst"]) for index, row in titles]

@task
def download_title(download_tuple):
    logger = prefect.context.get("logger")
    (downloader, tconst) = download_tuple
    try:
        downloader.download_title(tconst)
        logger.info("Successfully downloaded {} for {}".format(downloader.api_name, tconst))
        return downloader.api_name
    except JSONDecodeError as json_err:
        logger.error("Download of {} for {} failed: title not found".format(downloader.api_name, tconst))
        raise signals.FAIL(json_err)
    except Exception as err:
        logger.error("Exception occurred during download of {} for {}".format(downloader.api_name, tconst))
        raise err

@task(trigger=any_successful)
def get_processor_tasks(api, titles, data_dir, overwrite, chunk_index):
    logger = prefect.context.get("logger")
    logger.info("Creating processor task for api " + api["name"])
    basedir = os.path.join(data_dir, "process", api["name"])
    if not os.path.exists(basedir):
        os.makedirs(basedir)
    batch_outpath = os.path.join(basedir, "_{}-{}.parquet".format(api["name"], chunk_index))
    if not overwrite and os.path.exists(batch_outpath):
        logger.info("Batch {} for chunk {} already exists".format(api["name"], chunk_index))
        return []
    processor = Processor(api, api["parser"], data_dir)
    return [(processor, row["tconst"], row, batch_outpath) for index, row in titles]

@task
def process_title(process_tuple):
    logger = prefect.context.get("logger")
    (processor, tconst, row, batch_outpath) = process_tuple
    try:
        data = processor.process_title(tconst, row)
        logger.info("Successfully processed {} for {}".format(processor.api_name, tconst))
        title_dataframe = pd.DataFrame([data])
        return (batch_outpath, title_dataframe, processor.api_name)
    except Exception as err:
        if isinstance(err.args[0], str) and err.args[0].startswith("missing data dir"):
            logger.error("Processing of {} for {} failed: title not downloaded".format(processor.api_name, tconst))
            raise signals.FAIL(err.args[0])
        else:
            logger.error("Exception occurred during processing of {} for {}".format(processor.api_name, tconst))
            raise err

@task
def group_titles_by_batch(processor_results):
    batches = []
    batches_per_api = {}
    for batch_outpath, title_dataframe, api_name in processor_results:
        if not api_name in batches_per_api:
            batches_per_api[api_name] = {}
        if not batch_outpath in batches_per_api[api_name]:
            batches_per_api[api_name][batch_outpath] = []
        batches_per_api[api_name][batch_outpath].append(title_dataframe)

    # batches_per_api = {
    #     "credits": {
    #         "_credits-0.parquet": [df1,df2,df3],
    #         "_credits-1.parquet": [df4,df5,df6]
    #     },
    #     "business": {
    #         "_business-0.parquet": []
    #     }
    # }
    for api, batches_dict in batches_per_api.items():
        for batch_file, dataframes in batches_dict.items():
            batch_tuple = (api, batch_file, dataframes)
            batches.append(batch_tuple)
    return batches

@task
def create_batch_files(batch_tuple):
    logger = prefect.context.get("logger")
    (api_name, batch_file, dataframes) = batch_tuple
    logger.info("Creating batch file {}".format(batch_file))
    bdf = pd.concat(dataframes)
    bdf.to_parquet(batch_file)
    return (api_name, batch_file)

@task
def group_batches_by_api(batch_files):
    files_per_api = {}
    for (api_name, batch_file) in batch_files:
        if not api_name in files_per_api:
            files_per_api[api_name] = []
        files_per_api[api_name].append(batch_file)
    return list(files_per_api.items())

@task
def create_datasets(api_tuple, data_dir):
    logger = prefect.context.get("logger")
    (api_name, batch_files) = api_tuple
    logger.info("Creating dataset for API {}".format(api_name))
    ll = []
    for file in batch_files:
        ldf = pd.read_parquet(file)
        ll.append(ldf)
    df = pd.concat(ll)
    outpath = os.path.join(data_dir, "process", api_name, api_name + ".csv")
    df.to_csv(outpath)

@task
def log_stats(apis, titles, downloader_results, processor_results):
    logger = prefect.context.get("logger")
    stats = {}
    for api in apis:
        api_stats = {
            "titles": len(titles),
            "downloaded": len([api_name for api_name in downloader_results if api_name == api["name"]]),
            "processed": len([api_name for (batch_file, title_df, api_name) in processor_results if api_name == api["name"]])
        }
        stats[api["name"]] = api_stats
    logger.info("Statistics: {}".format(stats))

with Flow("IMDb Data Flow") as flow:
    input_apis = Parameter("apis", default=[])
    titles_file = Parameter("titles_file")
    data_directory = Parameter("data_directory")
    nginx_host = Parameter("nginx_host")
    overwrite = Parameter("overwrite")

    unfiltered_apis = get_api.map(input_apis, unmapped(data_directory))
    apis = filter_apis(unfiltered_apis)
    titles = get_titles_dataframe(titles_file)

    downloader_tasks = get_downloader_tasks.map(apis, unmapped(titles), unmapped(data_directory), unmapped(nginx_host), unmapped(overwrite))
    downloader_results = download_title.map(flatten(downloader_tasks))

    processor_tasks = get_processor_tasks.map(apis, unmapped(titles), unmapped(data_directory), unmapped(overwrite), unmapped(0))
    processor_tasks.set_upstream(downloader_results)
    unfiltered_processor_results = process_title.map(flatten(processor_tasks))
    processor_results = filter_failed_tasks(unfiltered_processor_results)

    #TODO remove batches and write directly on final file?
    titles_by_batch = group_titles_by_batch(processor_results)
    batch_files = create_batch_files.map(titles_by_batch)
    batches_by_api = group_batches_by_api(batch_files)
    create_datasets.map(batches_by_api, unmapped(data_directory))

    log_stats(apis, titles, filter_failed_tasks(downloader_results), processor_results)

if __name__=="__main__":
    flow.run(
        executor=LocalDaskExecutor(),
        parameters={
            "apis": ["credits", "fooo", "business"],
            "data_directory": "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp",
            "titles_file": "C:/Users/Erica.Tomaselli/pipelines_scripts/tmp/titles.csv",
            "nginx_host": "localhost",
            "overwrite": True
        }
    )
    flow.visualize()





#################################################################
# titles_dataframe = read_titles()
# apis = get_given_apis() #subset di api tra le api disponibili
# for api in apis:
#     #download
#     downloader = Downloader(api)
#     for title in titles_dataframe:
#         downloader.download_title(title) #genera file json per title
#     #process+parse
#     processor = Processor(api)
#     for a_chunk in chunk(titles_dataframe, CHUNKSIZE):
#         batch = []
#         for title in a_chunk:
#             data = processor.process_title(title, row)
#             batch.append(data)
#         create_partial_file(batch) #genera file parquet parziale per chunk
#     #store
#     for file in range(num_of_partial_files):
#         add_file_to_final_file()