import datetime
import glob
import os
import re
import tempfile
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


@dg.sensor(job_name="xrd_visualization_job", minimum_interval_seconds=600)
def girder_xrd_delta_sensor(
    context: dg.SensorEvaluationContext, girder: GirderConnection
):
    last_scan_date = context.cursor or "1970-01-01T00:00:00.000000+00:00"
    last_scan_date = dateutil.parser.parse(last_scan_date)
    new_cursor = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    ).isoformat()

    remote_updates = []

    for folder in girder.list_folders(os.environ.get("ROOT_FOLDER_ID")):
        raw_data_folder = last_update = None
        for raw_data_folder in girder.list_folders(folder["_id"], name="raw"):
            for i in girder.list_item(
                raw_data_folder["_id"], limit=1, sort="updated", sortdir=-1
            ):
                last_update = i["updated"]
        if not (raw_data_folder and last_update):
            continue

        if m := _date_time_pattern.search(folder["name"]):
            try:
                date, time = m.group(1), m.group(2)
                experiment_date = dateutil.parser.parse(
                    f"{date} {time.replace('-', ':')}+00:00"
                ).isoformat()
            except Exception as ex:
                print(f"Skipping {folder['name']} due to {ex}")
                continue
        igsn = folder["name"].split("_", 1)[0]
        if not igsn_pattern.match(igsn):
            igsn = default_igsn

        last_update = dateutil.parser.parse(last_update)
        if last_update > last_scan_date:
            remote_updates.append(
                (folder["_id"], igsn, experiment_date, raw_data_folder["_id"])
            )

    if not remote_updates:
        return None

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

    context.update_cursor(new_cursor)
    return run_requests


@dg.asset(
    partitions_def=experiment_partitions,
)
def xrd_data_visualization(context: dg.AssetExecutionContext, girder: GirderConnection):
    raw_folder_id = context.run.tags.get("girder/raw_data_folder_id")
    igsn = context.run.tags.get("igsn")
    experiment_date = context.run.tags.get("experiment_date")

    failed_scans = 0
    processed_scans = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        source_map = {}
        for item in girder.list_item(raw_folder_id):
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
        raw_folder = girder.client.getFolder(raw_folder_id)
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
