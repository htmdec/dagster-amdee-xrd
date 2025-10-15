import csv
import glob
import os
import tempfile
from importlib.metadata import version

import fabio
import matplotlib.pyplot as plt
import numpy as np
from pyFAI.detectors import Eiger2CdTe_1M
from pyFAI.integrator.azimuthal import AzimuthalIntegrator
from skimage import exposure


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


def analyze_xrd_scan(save_folder, logger):
    for master_file_name in glob.glob(f"{save_folder}/*_master.h5"):
        logger.info(f"Analyzing {master_file_name}")
        save_file_name = os.path.basename(master_file_name).replace(
            "_master.h5", "_xrd.png"
        )
        save_file_pathname = os.path.join(save_folder, save_file_name)

        image = fabio.open(master_file_name)
        ai = AzimuthalIntegrator(
            dist=0.0722579061017357,
            poni1=0.03968181491391674,
            poni2=0.03748027727247716,
            rot1=-0.01074087768419642,
            rot2=-0.009343428574425189,
            rot3=0.0,
            detector=Eiger2CdTe_1M(orientation=3),
            wavelength=5.142439e-11,
        )
        two_theta, intensity = ai.integrate1d(image.data, npt=1000)

        with plt.rc_context({"interactive": False}):
            fig, ax = plt.subplots(1, 1, figsize=(8, 8))
            ax.plot(two_theta, intensity, lw=1)
            ax.set_xlabel("2θ (degrees)")
            ax.set_ylabel("Intensity")
            ax.set_ylim(1, 500)
            ax.set_yscale("log")
            ax.set_xlim(25, 65)
            fig.tight_layout()
            fig.savefig(save_file_pathname, dpi=150)
            plt.close(fig)

        with plt.rc_context({"interactive": False}):
            fig, ax = plt.subplots(1, 1, figsize=(8, 8))
            ax.imshow(log_scale_and_contrast(image.data, 0.2), cmap="viridis")
            fig.tight_layout()
            fig.savefig(save_file_pathname.replace("_xrd.png", "_scan.png"), dpi=150)
            plt.close(fig)

        # dump the two theta and intensity data to a CSV file
        csv_file_pathname = save_file_pathname.replace("_xrd.png", "_xrd.csv")
        with open(csv_file_pathname, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Two Theta (degrees)", "Intensity"])
            for tt, intensity_value in zip(two_theta, intensity):
                writer.writerow([tt, intensity_value])


class XRDAnalysis:
    def __init__(self, dagster_context, folder_ids, girder_connection):
        self.context = dagster_context
        self.folder_ids = folder_ids
        self.girder = girder_connection
        self._items = None

    @property
    def items(self):
        def recurse_find(fid, root_id=None):
            for folder in self.girder.list_folder(fid):
                recurse_find(folder["_id"], root_id=root_id)

            for item in self.girder.list_item(fid):
                if item["name"].endswith(".h5"):
                    if item["name"].endswith("_master.h5"):
                        prefix = item["name"].split("_master")[0]
                    elif "_data_" in item["name"]:
                        prefix = item["name"].split("_data_")[0]
                    else:
                        continue

                    prefix = f"{root_id}-{prefix}"
                    if prefix not in self._items:
                        self._items[prefix] = {"master": None, "data": []}

                    if item["name"].endswith("_master.h5"):
                        self._items[prefix]["master"] = item
                    else:
                        self._items[prefix]["data"].append(item)

        if self._items is None:
            self._items = {}
            for folder_id in self.folder_ids:
                recurse_find(folder_id, root_id=folder_id)
        return self._items

    @property
    def version(self):
        return f"amdee_xrd-{version('amdee_xrd')}"

    def check_code_version(self, derivation):
        for item_id in derivation:
            item = self.girder.get_item(item_id)
            if (
                item.get("meta", {}).get("prov", {}).get("wasGeneratedBy")
                == self.version
            ):
                return True
        return False

    def analyze(self):
        for item in self.items.values():
            if item["master"] is None:
                self.context.log.error(f"No master file found {item}")
                continue

            if item["master"].get("meta", {}).get("prov", {}).get("hadDerivation"):
                if self.check_code_version(
                    item["master"]["meta"]["prov"]["hadDerivation"]
                ):
                    self.context.log.info(
                        f"Skipping {item['master']['name']} as it has already been analyzed"
                    )
                    continue

            outputs = []
            # Create a temporary directory
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download the master file
                master_file = self.girder.get_stream(item["master"]["_id"])
                with open(os.path.join(tmpdir, item["master"]["name"]), "wb") as f:
                    f.write(master_file.read())
                # Download the data files
                for data_file in item["data"]:
                    data = self.girder.get_stream(data_file["_id"])
                    with open(os.path.join(tmpdir, data_file["name"]), "wb") as f:
                        f.write(data.read())
                # Perform the analysis
                self.context.log.info(
                    f"Processing {item['master']['name']} in {tmpdir}"
                )
                analyze_xrd_scan(tmpdir, self.context.log)

                # Upload all generated png files
                for output_file in os.listdir(tmpdir):
                    if output_file.endswith(".h5"):
                        continue
                    self.context.log.info(f"Uploading {output_file}")
                    if output_file.endswith("png"):
                        mime_type = "image/png"
                    elif output_file.endswith("csv"):
                        mime_type = "text/csv"
                    else:
                        mime_type = "application/octet-stream"
                    output_upload = self.girder.upload_file_to_folder(
                        item["master"]["folderId"],
                        os.path.join(tmpdir, output_file),
                        mime_type=mime_type,
                        filename=output_file,
                    )
                    metadata = {
                        "runId": self.context.run.run_id,
                        "dataflowId": os.environ.get("DATAFLOW_ID", "unknown"),
                        "specId": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
                        "prov": {
                            "wasDerivedFrom": item["master"]["_id"],
                            "wasGeneratedBy": self.version,
                        },
                        "igsn": self.context.partition_key,
                    }
                    if output_upload is not None:
                        self.girder._client.addMetadataToItem(
                            output_upload["itemId"], metadata
                        )
                        outputs.append(output_upload["itemId"])
                    else:
                        self.context.log.error(f"Failed to upload {output_file}")

            # Add provenance metadata to the master and data files
            self.girder._client.addMetadataToItem(
                item["master"]["_id"],
                {
                    "prov": {
                        "hadDerivation": outputs,
                        "hasPart": [_["_id"] for _ in item["data"]],
                    },
                    "igsn": self.context.partition_key,
                },
            )
            for data_file in item["data"]:
                self.girder._client.addMetadataToItem(
                    data_file["_id"],
                    {
                        "prov": {"isPartOf": item["master"]["_id"]},
                        "igsn": self.context.partition_key,
                    },
                )
