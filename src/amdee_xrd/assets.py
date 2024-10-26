import glob
import os
import tempfile

from dagster import (
    asset,
    AutomationCondition,
    AssetExecutionContext,
    Config,
    DataVersion,
    DataVersionsByPartition,
    DynamicPartitionsDefinition,
    OpExecutionContext,
    RunConfig,
    RunRequest,
    ScheduleDefinition,
    SensorResult,
    define_asset_job,
    job,
    observable_source_asset,
    op,
    sensor,
)
from dagster_docker import docker_executor

from .resources import GirderConnection
from .utils import analyze_xrd_scan

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
    for sample in girder.list_folder(os.environ["DATAFLOW_SRC_FOLDER_ID"]):
        master_files = girder.master_files(sample["_id"])
        result[sample["name"]] = str(len(master_files))  # Use the number of items as the version, for now

    return DataVersionsByPartition(result)

observation_job = define_asset_job("observation_job", [xrd_samples])

observation_schedule = ScheduleDefinition(
    name="observation_schedule",
    cron_schedule="* * * * *",
    job=observation_job,
)

@asset(
    io_manager_key="girder_io_manager",
    partitions_def=xrd_samples_partitions_def,
    deps=[xrd_samples],
    automation_condition=AutomationCondition.any_deps_updated(),
)
def some_xrd_sample(context: AssetExecutionContext, girder: GirderConnection) -> None:
    context.log.info(f"Generating XRD plots for {context.partition_key}")
    folder = girder.folder_by_name(os.environ["DATAFLOW_SRC_FOLDER_ID"], context.partition_key)
    _analyze_xrd_sample(context, folder["_id"], girder)


class XRDSampleConfig(Config):
    folder_id: str


@op
def analyze_xrd_sample(
    context: OpExecutionContext, config: XRDSampleConfig, girder: GirderConnection
):
    context.log.info(f"Generating XRD plots for {config.folder_id}")
    _analyze_xrd_sample(context, config.folder_id, girder)

def _analyze_xrd_sample(context, folder_id, girder):
    files = {}
    items = girder.list_item(folder_id)
    for item in items:
        if item["name"].endswith("_master.h5"):
            prefix = item["name"].split("_master")[0]
        elif "_data_" in item["name"]:
            prefix = item["name"].split("_data_")[0]

        if prefix not in files:
            files[prefix] = {"master": None, "data": []}

        if item["name"].endswith("_master.h5"):
            files[prefix]["master"] = item
        else:
            files[prefix]["data"].append(item)

    for item in files.values():
        if item["master"] is None:
            context.log.error(f"No master file found {item}")
            continue
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download the master file
            master_file = girder.get_stream(item["master"]["_id"])
            with open(os.path.join(tmpdir, item["master"]["name"]), "wb") as f:
                f.write(master_file.read())
            # Download the data files
            for data_file in item["data"]:
                data = girder.get_stream(data_file["_id"])
                with open(os.path.join(tmpdir, data_file["name"]), "wb") as f:
                    f.write(data.read())
            # Perform the analysis
            context.log.info(f"Processing {item['master']['name']} in {tmpdir}")
            analyze_xrd_scan(tmpdir, context.log)

            # Upload all generated png files
            for png_file in glob.glob(os.path.join(tmpdir, "*.png")):
                context.log.info(f"Uploading {os.path.basename(png_file)}")
                girder.upload_file_to_folder(
                    item["master"]["folderId"],
                    png_file,
                    mime_type="image/png",
                    filename=os.path.basename(png_file),
                )


executor = docker_executor.configured(
    {
        "env_vars": [
            f"DATAFLOW_GIRDER_API_KEY={os.environ['DATAFLOW_GIRDER_API_KEY']}",
            f"DATAFLOW_GIRDER_API_URL={os.environ['DATAFLOW_GIRDER_API_URL']}",
            f"DATAFLOW_ID={os.environ['DATAFLOW_ID']}",
            f"DATAFLOW_SPEC_ID={os.environ['DATAFLOW_SPEC_ID']}",
            f"DATAFLOW_SRC_FOLDER_ID={os.environ['DATAFLOW_SRC_FOLDER_ID']}",
            f"DATAFLOW_DST_FOLDER_ID={os.environ['DATAFLOW_DST_FOLDER_ID']}",
        ],
        "container_kwargs": {
            "extra_hosts": {"girder.local.xarthisius.xyz": "host-gateway"}
        },
    }
)


# job(executor_def=executor)
@job
def generate_xrd_plots():
    analyze_xrd_sample()


def make_girder_folder_sensor(
    job, folder_id, sensor_name, partitions_def: DynamicPartitionsDefinition
):
    @sensor(name=sensor_name, job=job)
    def folder_contents(context, girder: GirderConnection):
        new_folders = []
        context.log.info(f"Checking for new XRD samples in folder {folder_id}")
        for folder in girder.list_folder(folder_id):
            context.log.info(f"Checking if {folder['name']} is in partition")
            if not context.instance.has_dynamic_partition(
                partitions_def.name, folder["name"]
            ):
                new_folders.append(folder)
        run_requests = []
        for folder in new_folders:
            tags = partitions_def.get_tags_for_partition_key(folder["name"])
            tags["folderId"] = folder["_id"]
            run_requests.append(
                RunRequest(
                    partition_key=folder["name"],
                    tags=tags,
                    run_config=RunConfig(
                        ops={
                            "analyze_xrd_sample": XRDSampleConfig(
                                folder_id=folder["_id"]
                            )
                        }
                    ),
                )
            )
        return SensorResult(
            run_requests=[],  #run_requests,
            dynamic_partitions_requests=[
                partitions_def.build_add_request([_["name"] for _ in new_folders])
            ],
        )

    return folder_contents
