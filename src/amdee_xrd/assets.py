import os
import warnings
import dagster

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    DataVersion,
    DataVersionsByPartition,
    DynamicPartitionsDefinition,
    OpExecutionContext,
    ScheduleDefinition,
    asset,
    define_asset_job,
    job,
    multiprocess_executor,
    observable_source_asset,
    op,
)
from dagster_docker import docker_executor

from .resources import GirderConnection
from .utils import XRDAnalysis

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)
xrd_samples_partitions_def = DynamicPartitionsDefinition(name="xrd_samples")


def xrd_samples_check(context) -> DataVersion:
    return DataVersionsByPartition({"xrd_samples": "1"})


@observable_source_asset(
    io_manager_key="girder_io_manager",
    partitions_def=xrd_samples_partitions_def,
)
def xrd_samples(
    context: AssetExecutionContext, girder: GirderConnection
) -> DataVersionsByPartition:
    result = {}
    for sample_folder in girder.sample_folders():
        if sample_folder["name"] not in xrd_samples_partitions_def.get_partition_keys(
            dynamic_partitions_store=context.instance
        ):
            context.instance.add_dynamic_partitions(
                xrd_samples_partitions_def.name, [sample_folder["name"]]
            )
        master_files = girder.master_files(sample_folder["_id"])
        unprocessed = [
            file
            for file in master_files
            if not file.get("meta", {}).get("prov", {}).get("hadDerivation")
        ]
        result[sample_folder["name"]] = f"{len(master_files)}-{len(unprocessed)}"

    return DataVersionsByPartition(result)


observation_job = define_asset_job(
    "observation_job",
    selection=[xrd_samples],
    executor_def=multiprocess_executor,
)

observation_schedule = ScheduleDefinition(
    name="observation_schedule",
    cron_schedule="* * * * *",
    job=observation_job,
)


@asset(
    io_manager_key="girder_io_manager",
    partitions_def=xrd_samples_partitions_def,
    deps=[xrd_samples],
    automation_condition=AutomationCondition.any_deps_updated() | AutomationCondition.missing(),
)
def some_xrd_sample(context: AssetExecutionContext, girder: GirderConnection) -> None:
    context.log.info(f"Generating XRD plots for {context.partition_key}")
    sample_folder = girder.sample_by_name(context.partition_key)
    XRDAnalysis(context, sample_folder["_id"], girder).analyze()


class XRDSampleConfig(Config):
    folder_id: str


@op
def analyze_xrd_sample(
    context: OpExecutionContext, config: XRDSampleConfig, girder: GirderConnection
):
    context.log.info(f"Generating XRD plots for {config.folder_id}")
    XRDAnalysis(context, config.folder_id, girder).analyze()


executor = docker_executor.configured(
    {
        "env_vars": [
            f"GIRDER_API_KEY={os.environ['GIRDER_API_KEY']}",
            f"GIRDER_API_URL={os.environ['GIRDER_API_URL']}",
            f"DATAFLOW_ID={os.environ['DATAFLOW_ID']}",
            f"DATAFLOW_SPEC_ID={os.environ['DATAFLOW_SPEC_ID']}",
            f"DATAFLOW_SRC_FOLDER_ID={os.environ['DATAFLOW_SRC_FOLDER_ID']}",
            f"DATAFLOW_DST_FOLDER_ID={os.environ['DATAFLOW_DST_FOLDER_ID']}",
        ],
        "container_kwargs": {
            "auto_remove": True,
            "extra_hosts": {"girder.local.xarthisius.xyz": "host-gateway"},
        },
    }
)


@job
def generate_xrd_plots():
    analyze_xrd_sample()
