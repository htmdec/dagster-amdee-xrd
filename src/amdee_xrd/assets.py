import os
import warnings
import dagster

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    DataVersionsByPartition,
    DynamicPartitionsDefinition,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multiprocess_executor,
    observable_source_asset,
)
from dagster_docker import docker_executor

from .resources import GirderConnection
from .utils import XRDAnalysis

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)
xrd_samples_partitions_def = DynamicPartitionsDefinition(name="xrd_samples")


@observable_source_asset(
    io_manager_key="girder_io_manager",
    partitions_def=xrd_samples_partitions_def,
    description="All XRD samples available in Girder",
)
def raw_xrd_data(
    context: AssetExecutionContext, girder: GirderConnection
) -> DataVersionsByPartition:
    result = {}
    sample_folders = girder.sample_folders()
    new_samples = [
        sample_id
        for sample_id in sample_folders.keys()
        if sample_id
        not in xrd_samples_partitions_def.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    ]
    if new_samples:
        context.instance.add_dynamic_partitions(
            xrd_samples_partitions_def.name, new_samples
        )

    for sample_id, meta in sample_folders.items():
        master_files = []
        for folder_id in meta["folders"]:
            master_files += girder.master_files(folder_id)

        unprocessed = [
            file
            for file in master_files
            if not file.get("meta", {}).get("prov", {}).get("hadDerivation")
        ]
        result[sample_id] = f"{len(master_files)}-{len(unprocessed)}"

    return DataVersionsByPartition(result)


observation_job = define_asset_job(
    "observation_job",
    selection=[raw_xrd_data],
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
    deps=[raw_xrd_data],
    automation_condition=AutomationCondition.any_deps_updated()
    | AutomationCondition.missing(),
)
def xrd_samples(context: AssetExecutionContext, girder: GirderConnection) -> None:
    context.log.info(f"Generating XRD plots for {context.partition_key}")
    sample_folders = girder.sample_folders()[context.partition_key]
    XRDAnalysis(context, sample_folders["folders"], girder).analyze()


class XRDSampleConfig(Config):
    folder_id: str


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
