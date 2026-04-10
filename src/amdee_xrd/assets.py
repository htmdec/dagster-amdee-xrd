import datetime

import dagster as dg

from .resources import GirderConnection
from .utils import XRDAnalysis

_ACTIVE_RUN_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]

experiment_partitions = dg.DynamicPartitionsDefinition(name="xrd_experiment_runs")


@dg.sensor(job_name="xrd_reduction_job", minimum_interval_seconds=60)
def girder_xrd_delta_sensor(
    context: dg.SensorEvaluationContext, girder: GirderConnection
):
    """
    Uses the 'since' kwarg to only poll for modified experiments.
    """
    last_poll_time = context.cursor or "1970-01-01T00:00:00.000000+00:00"

    remote_updates = girder.list_partitions(data_type="xrd_raw", since=last_poll_time)

    if not remote_updates:
        return None

    new_partition_keys = []
    run_requests = []

    # We'll use the current system time (or Girder server time) for the next cursor
    # To be safe, we use the timestamp of the latest 'updated' file if available,
    # but here we'll use the execution time minus a small buffer for safety.
    new_cursor = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    ).isoformat()

    existing_partitions = context.instance.get_dynamic_partitions(
        experiment_partitions.name
    )

    for partition_key, checksum in remote_updates.items():
        if partition_key not in existing_partitions:
            new_partition_keys.append(partition_key)
            existing_partitions.append(partition_key)  # Avoid dups in this loop

        active_runs = context.instance.get_runs(
            filters=dg.RunsFilter(
                job_name="xrd_reduction_job",
                statuses=_ACTIVE_RUN_STATUSES,
                tags={"dagster/partition": partition_key},
            )
        )
        if active_runs:
            context.log.debug(
                f"Skipping partition {partition_key!r}: run {active_runs[0].run_id} is already active."
            )
            continue

        run_requests.append(
            dg.RunRequest(partition_key=partition_key, tags={"data_checksum": checksum})
        )

    if new_partition_keys:
        context.instance.add_dynamic_partitions(
            experiment_partitions.name, new_partition_keys
        )

    context.update_cursor(new_cursor)
    return run_requests


@dg.asset(
    partitions_def=experiment_partitions,
)
def xrd_data_reduction(context: dg.AssetExecutionContext, girder: GirderConnection):
    partition_key = context.partition_key
    items = girder.get_partition(partition_key)

    if not items:
        return

    # there are two file types: _master.h5 and _data_*.h5. We need to iterate over _master files
    # and find the corresponding _data_ files for processing. They share a common prefix
    # scan_point_<n>_master.h5 and scan_point_<n>_data_<m>.h5. Group files by prefix
    grouped_items = {}
    for item in items:
        try:
            scan_no = int(item["name"].split("_")[2])
        except (IndexError, ValueError, TypeError):
            context.log.warning(
                f"Skipping file with unexpected name format: {item['name']}"
            )
            continue
        if scan_no not in grouped_items:
            grouped_items[scan_no] = {"master": None, "data": []}
        if item["name"].endswith("_master.h5"):
            grouped_items[scan_no]["master"] = item
        elif "_data_" in item["name"]:
            grouped_items[scan_no]["data"].append(item)

    failed_scans = []
    for prefix, group in grouped_items.items():
        if not group["master"]:
            context.log.warning(f"Missing master file for scan point {prefix}")
            failed_scans.append(prefix)
            continue
        if not group["data"]:
            context.log.warning(f"Missing data files for scan point {prefix}")
            failed_scans.append(prefix)
            continue

        XRDAnalysis(context, group, girder).analyze()

    if failed_scans:
        return dg.MaterializeResult(
            metadata={"failed_scans": dg.MetadataValue.json({"scan_no": failed_scans})}
        )


xrd_reduction_job = dg.define_asset_job(
    name="xrd_reduction_job", selection=dg.AssetSelection.assets(xrd_data_reduction)
)
