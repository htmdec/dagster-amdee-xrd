import datetime
import glob
import json
import os
import re
import tempfile
import time
from importlib.metadata import version

import dagster as dg
import dateutil.parser
import fabio
import matplotlib.pyplot as plt
import numpy as np
from skimage import exposure

from .resources import GirderConnection

_ACTIVE_RUN_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]

experiment_partitions = dg.DynamicPartitionsDefinition(name="xrd_experiment_runs")

_date_time_pattern = re.compile(
    r"(?<![0-9])(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})(?![0-9])"
)
igsn_pattern = re.compile(r"^[A-Z]{6}[0-9]{5}[A-Z0-9\-]*$", re.IGNORECASE)
default_igsn = "JHABOX00000"


def log_scale_and_contrast(
    intensity_array: np.ndarray, saturation_percent: float
) -> np.ndarray:
    """
    Applies a logarithmic scale and adjusts the contrast of a 2D array.

    Args:
        intensity_array (np.ndarray): The input 2D numpy array (e.g., an image).
                                      Values should be non-negative.
        saturation_percent (float): The percentage of pixels to saturate at both the low and high ends.
                                    For example, 0.2 means saturate the bottom 0.2% and top 0.2%.

    Returns:
        np.ndarray: The processed array with log scaling and contrast adjustment.
    """
    if np.min(intensity_array) < 0:
        print("Warning: Input array contains negative values. Clipping to 0.")
        intensity_array = np.clip(intensity_array, 0, None)

    print("Applying logarithmic scale...")
    log_scaled_array = np.log1p(intensity_array)

    lower_percentile = saturation_percent
    higher_percentile = 100 - saturation_percent

    print(
        f"Calculating intensity values at {lower_percentile}% and {higher_percentile}% percentiles..."
    )
    p_low, p_high = np.percentile(
        log_scaled_array, (lower_percentile, higher_percentile)
    )

    print("Rescaling intensity for contrast adjustment...")
    contrast_adjusted_array = exposure.rescale_intensity(
        log_scaled_array, in_range=(p_low, p_high)
    )

    return contrast_adjusted_array


# Dagster kills a sensor tick that runs longer than 60s. Both bounds below keep
# us well inside that; whatever is left over is picked up by the next tick, as
# the cursor only advances past folders we actually handled.
_MAX_FOLDERS_PER_TICK = 50
_TICK_BUDGET_SECONDS = 35

# The asset only ever renders ``*_master.h5``, so that is all we need to watch.
_MASTER_FILE_QUERY = {"name": {"$regex": r"_master\.h5$"}}


def _object_id_floor(moment: datetime.datetime) -> str:
    """Smallest ObjectId that could have been generated at ``moment``.

    Girder's item collection is indexed on ``_id``, and ObjectIds embed their
    creation time in the leading 4 bytes, so a range query on ``_id`` is an
    indexed stand-in for "created after". Querying ``updated`` directly is a
    full collection scan (~30s on this instance) and blows the tick budget.
    """
    return f"{int(moment.timestamp()):08x}" + "0" * 16


def _parse_cursor(cursor: str | None) -> str:
    """Return the exclusive ``_id`` lower bound encoded in ``cursor``."""
    if not cursor:
        return _object_id_floor(
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        )
    if cursor.startswith("{"):
        return json.loads(cursor)["since_id"]
    # Legacy cursor: a bare ISO timestamp.
    return _object_id_floor(dateutil.parser.parse(cursor))


def _make_cursor(since_id: str) -> str:
    seconds = int(since_id[:8], 16)
    since = datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc)
    return json.dumps({"since_id": since_id, "since": since.isoformat()})


def _experiment_date(folder_name: str) -> str | None:
    if not (m := _date_time_pattern.search(folder_name)):
        return None
    date, time = m.group(1), m.group(2)
    try:
        return dateutil.parser.parse(
            f"{date} {time.replace('-', ':')}+00:00"
        ).isoformat()
    except Exception:
        return None


@dg.sensor(job_name="xrd_visualization_job", minimum_interval_seconds=240)
def girder_xrd_delta_sensor(
    context: dg.SensorEvaluationContext, girder: GirderConnection
):
    root_folder_id = os.environ.get("ROOT_FOLDER_ID")
    since_id = _parse_cursor(context.cursor)
    # Ignore the last few minutes so an in-flight upload isn't picked up halfway.
    new_since_id = _object_id_floor(
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    )

    query = dict(_MASTER_FILE_QUERY)
    query["_id"] = {"$gt": {"$oid": since_id}, "$lte": {"$oid": new_since_id}}

    # Newly created master files, oldest first, grouped by the folder holding
    # them (i.e. the ``raw`` folder of one experiment). Alongside each folder we
    # keep the id of the last item seen before it showed up, so that bailing out
    # before that folder leaves a cursor which rediscovers it next tick.
    deadline = time.monotonic() + _TICK_BUDGET_SECONDS
    candidates = []
    seen_folder_ids = set()
    last_item_id = since_id
    for item in girder.query_items(query, sort="_id", sortdir=1):
        if item["folderId"] not in seen_folder_ids:
            if len(candidates) >= _MAX_FOLDERS_PER_TICK:
                context.log.info(
                    f"Reached the {_MAX_FOLDERS_PER_TICK} folder per-tick cap; "
                    "the rest follows next tick."
                )
                new_since_id = last_item_id
                break
            seen_folder_ids.add(item["folderId"])
            candidates.append((item["folderId"], last_item_id))
        last_item_id = item["_id"]

    if not candidates:
        context.update_cursor(_make_cursor(new_since_id))
        return dg.SkipReason("No new XRD scans since the last tick.")

    remote_updates = []
    for position, (raw_folder_id, boundary_item_id) in enumerate(candidates):
        # Always resolve the first candidate, otherwise the cursor never moves.
        if position and time.monotonic() > deadline:
            context.log.info(
                f"Out of tick budget after {position} of {len(candidates)} folders; "
                "the rest follows next tick."
            )
            new_since_id = boundary_item_id
            break

        raw_folder = girder.get_folder(raw_folder_id)
        if raw_folder["name"] != "raw":
            continue
        folder = girder.get_folder(raw_folder["parentId"])
        if folder["parentId"] != root_folder_id:
            continue

        experiment_date = _experiment_date(folder["name"])
        if not experiment_date:
            context.log.warning(
                f"Skipping {folder['name']!r}: no experiment date in the folder name."
            )
            continue

        igsn = folder["name"].split("_", 1)[0]
        if not igsn_pattern.match(igsn):
            igsn = default_igsn

        remote_updates.append((folder["_id"], igsn, experiment_date, raw_folder["_id"]))

    new_partition_keys = []
    run_requests = []
    existing_partitions = context.instance.get_dynamic_partitions(
        experiment_partitions.name
    )
    for folder_id, igsn, experiment_date, raw_data_folder_id in remote_updates:
        partition_key = folder_id

        if partition_key not in existing_partitions:
            new_partition_keys.append(partition_key)
            existing_partitions.append(partition_key)  # Avoid dups in this loop

        active_runs = context.instance.get_runs(
            filters=dg.RunsFilter(
                job_name="xrd_visualization_job",
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
            dg.RunRequest(
                partition_key=partition_key,
                tags={
                    "girder/raw_data_folder_id": raw_data_folder_id,
                    "igsn": igsn,
                    "experiment_date": experiment_date,
                },
            )
        )

    if new_partition_keys:
        context.instance.add_dynamic_partitions(
            experiment_partitions.name, new_partition_keys
        )

    context.update_cursor(_make_cursor(new_since_id))
    return run_requests


@dg.asset(
    partitions_def=experiment_partitions,
)
def xrd_data_visualization(context: dg.AssetExecutionContext, girder: GirderConnection):
    parent = girder.client.getFolder(context.partition_key)
    raw_folder_id = context.run.tags.get("girder/raw_data_folder_id")
    if not raw_folder_id:
        try:
            raw_folder = next(girder.list_folders(parent["_id"], name="raw"))
        except Exception:
            context.log.error("No 'raw' data found")
            return
    else:
        raw_folder = girder.client.getFolder(raw_folder_id)
    igsn = context.run.tags.get("igsn")
    if not igsn:
        igsn = parent["name"].split("_", 1)[0]
        if not igsn_pattern.match(igsn):
            igsn = default_igsn

    experiment_date = context.run.tags.get("experiment_date")
    if not experiment_date:
        if m := _date_time_pattern.search(parent["name"]):
            try:
                date, time = m.group(1), m.group(2)
                experiment_date = dateutil.parser.parse(
                    f"{date} {time.replace('-', ':')}+00:00"
                ).isoformat()
            except Exception as ex:
                context.log.error(f"Skipping {parent['name']} due to {ex}")
                return

    context.log.info(
        f"Running workflow for raw_folder_id={raw_folder['_id']}, "
        f"igsn={igsn}, experiment_date={experiment_date}"
    )
    failed_scans = 0
    processed_scans = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        source_map = {}
        for item in girder.list_item(raw_folder["_id"]):
            source_map[item["name"]] = item["_id"]
            girder.client.downloadItem(item["_id"], tmpdir)

            # upload item to folder
        for master_file_name in glob.glob(os.path.join(tmpdir, "*_master.h5")):
            try:
                context.log.info(f"Analyzing {master_file_name}")
                save_file_name = os.path.basename(master_file_name).replace(
                    "_master.h5", "_scan.jpg"
                )
                save_file_pathname = os.path.join(tmpdir, save_file_name)

                image = fabio.open(master_file_name)
                with plt.rc_context({"interactive": False}):
                    fig, ax = plt.subplots(1, 1, figsize=(5, 5), dpi=150)
                    ax.imshow(log_scale_and_contrast(image.data, 0.2), cmap="viridis")
                    ax.set_axis_off()
                    fig.tight_layout()
                    fig.savefig(save_file_pathname)
                    plt.close(fig)
                processed_scans += 1
            except Exception as exc:
                context.log.error(
                    f"Processing {master_file_name} failed with {str(exc)}"
                )
                failed_scans += 1

        dagster_flow_version = version("amdee_xrd")
        for output_file in glob.glob(os.path.join(tmpdir, "*.jpg")):
            filename = os.path.basename(output_file)
            source_name = filename.replace("_scan.jpg", "_master.h5")
            item = girder.client.createItem(
                raw_folder["parentId"],
                os.path.basename(output_file),
                metadata={
                    "igsn": igsn,
                    "experiment_date": experiment_date,
                    "data_type": "xrd_visualization",
                    "prov": {
                        "wasGeneratedBy": f"dagster-amdee-xrd/{dagster_flow_version}",
                        "wasDerivedFrom": source_map[source_name],
                    },
                },
                reuseExisting=True,
            )
            girder.client.uploadFileToItem(
                item["_id"], output_file, mimeType="image/jpeg"
            )

    return dg.MaterializeResult(
        metadata={
            "failed_scans": dg.MetadataValue.int(failed_scans),
            "processed_scans": dg.MetadataValue.int(processed_scans),
        }
    )


xrd_visualization_job = dg.define_asset_job(
    name="xrd_visualization_job",
    selection=dg.AssetSelection.assets(xrd_data_visualization),
)
