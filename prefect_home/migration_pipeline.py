# Migration machine pipeline that uses both functional and imperative API
# The configuration file path must be specified inside .prefect/config.toml as:
# migration_machine_config_file = "path/to/transform.ini"
import os
import json
import pandas as pd
import migration_machine.config as configurations
from migration_machine.parser.source_parser import SourceParser
from migration_machine.mapper.mapping_processor import Mapper
from migration_machine.merger.merger_pandas import Merger

import prefect
from prefect import task, Task, Flow, unmapped
from prefect.executors import LocalDaskExecutor

####### tasks and Task classes
class ParserTask(Task):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        super().__init__(*args, **kwargs)

    def run(self):
        self.logger.info("Instantiating parser")
        return SourceParser(self.config)

class MapperTask(Task):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        super().__init__(*args, **kwargs)

    def run(self):
        self.logger.info("Instantiating mapper")
        return Mapper(self.config)

class MergerTask(Task):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        super().__init__(*args, **kwargs)

    def run(self):
        self.logger.info("Instantiating merger")
        return Merger(self.config)

class DatasetTask(Task):
    def __init__(self, dataset, *args, **kwargs):
        self.dataset = dataset
        super().__init__(*args, **kwargs)
    
    def run(self):
        self.logger.info("Dataset: " + self.dataset)
        return self.dataset

@task
def get_config_tuple(config, parser):
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

@task
def read_source_schema(config_tuple, parser):
    parse_args = []
    (
        domain_data_path,
        format_convert,
        packagename,
        source_schema_path,
        transform,
        datapackage,
        dest_data_file_path
    ) = config_tuple
    df = pd.read_fwf(source_schema_path, header=None, widths=[10, 10, 50, 10, 50, 5, 10], encoding="ISO-8859-1")
    dfg = df.groupby([0, 1, 2])
    for key in dfg.groups:
        file_path = os.path.join(domain_data_path, key[1] + "_" + key[1])
        if os.path.exists(file_path):
            file_name = key[1] + "_" + key[1]
            names = parser.output_schema(file_name, datapackage, dfg, format_convert, key, packagename)
            #append args required to parse this file
            parse_args.append({
                "file_name": file_name,
                "file_path": domain_data_path + "/" + file_name,
                "dest_data_file_path": dest_data_file_path,
                "format_convert": format_convert,
                "key": key,
                "names": names,
                "transform": transform
            })
    return parse_args

@task
def parse_source_file(args_dict, parser):
    logger = prefect.context.get("logger")
    logger.info("Parsing " + args_dict["file_name"])
    parser.output_format(
        args_dict["file_name"],
        args_dict["file_path"],
        args_dict["dest_data_file_path"],
        args_dict["format_convert"],
        args_dict["key"],
        args_dict["names"],
        args_dict["transform"],
    )
    logger.info("Parsed " + args_dict["file_name"])

@task
def execute_mapping(dataset, spec_file, mapping_index, mapper):
    logger = prefect.context.get("logger")
    logger.info("Mapping " + dataset + "[" + str(mapping_index) + "]")
    mapper.execute_mapping_file(spec_file, mapping_index)

@task
def merge_dataset(dataset, merger):
    logger = prefect.context.get("logger")
    logger.info("Merging " + dataset)
    merger.run_merger_single_dataset(dataset)

####### instantiation of Task classes and flow
config = configurations.Config(prefect.config.migration_machine_config_file)
settings_path = config.domain["domain_settings_path"]

parser = ParserTask(config)
mapper = MapperTask(config)
merger = MergerTask(config)

flow = Flow("Imperative Migration Flow")

with flow:
    config_tuple = get_config_tuple(config, parser)
    parse_args = read_source_schema(config_tuple, parser)
    parser_results = parse_source_file.map(args_dict=parse_args, parser=unmapped(parser))

    #create a task for each dataset and give parser_results as upstream
    for dataset in os.listdir(settings_path):
        if os.path.isdir(os.path.join(settings_path, dataset)):
            dataset_task = DatasetTask(dataset)
            dataset_task.set_upstream(parser_results)

            merge_task = merge_dataset(dataset_task, merger)

            specifications_file = os.path.join(settings_path, dataset, "migration_" + dataset + "_result.json")
            with open(specifications_file) as file:
                spec = json.load(file)
            #create a task for each mapping and give dataset as upstream
            for mapping in spec["mappings"]:
                mapping_task = execute_mapping(dataset_task, specifications_file, mapping["mapping_index"], mapper)
                mapping_task.set_downstream(merge_task)

if __name__=="__main__":
    flow.run(executor=LocalDaskExecutor())
    flow.visualize()
