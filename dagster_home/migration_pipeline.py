import os
import json
import pandas as pd

import migration_machine.config as configurations
from migration_machine.parser.source_parser import SourceParser
from migration_machine.mapper.mapping_processor import Mapper
from migration_machine.merger.merger_pandas import Merger

from dagster import job, op, graph, Out, DynamicOut, DynamicOutput, get_dagster_logger

@op(out={
    "domain_data_path": Out(),
    "format_convert": Out(),
    "packagename": Out(),
    "source_schema_path": Out(),
    "transform": Out(),
    "datapackage": Out(),
    "dest_data_file_path": Out()},
    config_schema={"config_path": str})
def get_config_tuple(context):
    config = configurations.Config(context.op_config["config_path"])
    parser = SourceParser(config)
    (domain_data_path, format_convert, output, packagename, regenerate_schema, source_schema_path, transform) = parser.read_config(config)
    datapackage = {"name": packagename, "resources": []}
    dest_data_file_path = domain_data_path + "/" + format_convert
    os.makedirs(dest_data_file_path, exist_ok=True)
    return (
        domain_data_path,
        format_convert,
        packagename,
        source_schema_path,
        transform,
        datapackage,
        dest_data_file_path
    )

@op
def get_dataframe_group(source_schema_path):
    df = pd.read_fwf(source_schema_path, header=None, widths=[10, 10, 50, 10, 50, 5, 10], encoding="ISO-8859-1")
    dfg = df.groupby([0, 1, 2])
    return dfg

@op(out=DynamicOut())
def get_dataframes(dfg, domain_data_path):
    for key in dfg.groups:
        file_path = os.path.join(domain_data_path, key[1] + "_" + key[1])
        if os.path.exists(file_path):
            yield DynamicOutput(
                value = key,
                mapping_key = key[1]
            )

@op(config_schema={"config_path": str})
def parse_source_files(context, key, datapackage, dfg, format_convert, packagename, domain_data_path, dest_data_file_path, transform):
    config = configurations.Config(context.op_config["config_path"])
    parser = SourceParser(config)
    data_file_name = key[1] + "_" + key[1]
    names = parser.output_schema(data_file_name, datapackage, dfg, format_convert, key, packagename)
    data_file_path = domain_data_path + "/" + data_file_name
    parser.output_format(
        data_file_name,
        data_file_path,
        dest_data_file_path,
        format_convert,
        key,
        names,
        transform,
    )
    return 1

@op(out=DynamicOut(), config_schema={"config_path": str})
def get_datasets(context, parser_results):
    config = configurations.Config(context.op_config["config_path"])
    mapper = Mapper(config)
    settings_path = mapper.configs.domain["domain_settings_path"]
    datasets_folders = os.listdir(settings_path)
    for dataset in datasets_folders:
        if os.path.isdir(os.path.join(settings_path, dataset)):
            yield DynamicOutput(
                value = dataset,
                mapping_key = dataset
            )

'''
@op(out=DynamicOut())
def get_mappings(dataset):
    config = configurations.Config("C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini")
    mapper = Mapper(config)
    settings_path = mapper.configs.domain["domain_settings_path"]
    mapper_specifications_file = os.path.join(settings_path, dataset, "migration_" + dataset + "_result.json")
    with open(mapper_specifications_file) as file:
        spec = json.load(file)
    for mapping in spec["mappings"]:
        yield DynamicOutput(
            value = {
                "dataset": dataset,
                "spec_file": mapper_specifications_file,
                "mapping_index": mapping["mapping_index"]
            },
            mapping_key = dataset + mapping["mapping_index"]
        )

@op
def execute_mapping(mapping):
    config = configurations.Config("C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini")
    mapper = Mapper(config)
    mapper.execute_mapping_file(mapping["spec_file"], mapping["mapping_index"])
    return 1

@op
def merge(dataset, dataset_mappings):
    config = configurations.Config("C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini")
    merger = Merger(config)
    merger.run_merger_single_dataset(dataset)

@graph
def mapping_graph(dataset):
    dataset_mappings = get_mappings(dataset).map(execute_mapping)
    merge(dataset, dataset_mappings.collect())
'''

@op(config_schema={"config_path": str})
def execute_dataset_mapping(context, dataset):
    logger = get_dagster_logger()
    logger.info(f"mapping dataset: {dataset}")
    config = configurations.Config(context.op_config["config_path"])
    mapper = Mapper(config)
    settings_path = mapper.configs.domain["domain_settings_path"]
    mapper_specifications_file = os.path.join(settings_path, dataset, "migration_" + dataset + "_result.json")

    with open(mapper_specifications_file) as file:
        spec = json.load(file)
    mapper.execute_mapping_file(mapper_specifications_file)
    return dataset

@op(config_schema={"config_path": str})
def merge_dataset(context, dataset):
    logger = get_dagster_logger()
    logger.info(f"merging dataset: {dataset}")
    config = configurations.Config(context.op_config["config_path"])
    merger = Merger(config)
    merger.run_merger_single_dataset(dataset)

@job
def migrate():
    (
        domain_data_path,
        format_convert,
        packagename,
        source_schema_path,
        transform,
        datapackage,
        dest_data_file_path
    ) = get_config_tuple()
    dfg = get_dataframe_group(source_schema_path)
    dataframes = get_dataframes(dfg, domain_data_path)
    parser_results = dataframes.map(lambda key: parse_source_files(key, datapackage, dfg, format_convert, packagename, domain_data_path, dest_data_file_path, transform))
    #
    #mappings = get_mappings(parser_results.collect())
    #mappings.map(execute_mapping)
    #
    datasets = get_datasets(parser_results.collect())
    datasets.map(execute_dataset_mapping).map(merge_dataset)


'''
ops:
  get_config_tuple:
    config:
      config_path: "C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini"
  parse_source_files:
    config:
      config_path: "C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini"
  get_datasets:
    config:
      config_path: "C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini"
  execute_dataset_mapping:
    config:
      config_path: "C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini"
  merge_dataset:
    config:
      config_path: "C:/Users/Erica.Tomaselli/mm-processor/processor/examples/SIB2SICRA/transform.ini"
'''
