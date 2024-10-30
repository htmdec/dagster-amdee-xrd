import glob
import os
import tempfile
from importlib.metadata import version
from math import pi

import h5py
import hdf5plugin  # noqa: F401
import numpy as np
from matplotlib import pyplot as plt
from palettable import cubehelix

from .image_utilities import azimuthal_integration, preprocess_image

colormap = cubehelix.get_map("classic_16", reverse=True).get_mpl_colormap()
# image collection and save path setting
xray_technique = "XRD"  # permitted: 'XRD', 'XRF', 'XRDXRF'
exposure_time_seconds = 10

# detector parameters
det_dist = (
    100
)  # order of magnitude approximation                              # milimeters
det_pix_dim = np.array([1062, 1028])  # pixels, [y,x]
det_pix_size = 0.075  # milimeters
det_dim = np.array([79.7, 77.1])  # milimeters
det_psf_FWHM = 2  # FWHM (in pixels) of the point spread function
det_centers = np.array([-0.473, -1.02])  # offset (in pixels) from 0.5 * det_pix_dim
det_tilts = np.array([0, 0])  # degrees


# integration (aka "cake") parameters
# two-theta bin width for azimuthally integration (degrees)
tth_bin_size = 0.05
max_tth = 20.0
min_tth = 2.0
r_ring_half_width_bins = 5

# sector integration parameters
#   Polar variables have units of degrees
eta_slice_width = 10.0
eta_fine_bin_size = 1.0
tth_ring_width = 0.5
tth_fine_bin_size = 0.1
r_bin_size_pix = 1

# ---------------#
#     Setup     #
# ---------------#

# parsing inputs

# build dictionaries
detector = {
    "distance": det_dist,
    "pix_dim": det_pix_dim,
    "pix_size": det_pix_size,
    "dim": det_dim,
    "psf_FWHM": det_psf_FWHM,
    "centers": det_centers,
    "tilts": det_tilts,
}
# 36 bins = 10 degrees per bin
num_eta_bins = int(360 / eta_slice_width)
cake_params = {
    "eta_slice_width": eta_slice_width,
    "eta_fine_bin_size": eta_fine_bin_size,
    "num_eta_bins": num_eta_bins,
    "tth_ring_width": tth_ring_width,
    "tth_bin_size": tth_bin_size,
    "max_tth": max_tth,
    "tth_fine_bin_size": tth_fine_bin_size,
    "r_bin_size_pix": r_bin_size_pix,
}

# convert min tth to min r
min_r = np.tan(min_tth * pi / 180) * detector["distance"]


# function definitions
# image loading function
def load_eiger_image_h5(filepath):
    """
    load_eiger_image_h5: Loads the Eiger detector image in the given h5 file.

    USAGE:
        img = load_eiger_image_h5(filepath)

    INPUTS:
        filepath : a path or path-like (string)
            The full path to the file containing the image to be loaded.  Should end in "_master.h5"

    RETURNS:
        img
    """
    with h5py.File(filepath, "r") as f:
        return f["entry"]["data"]["data_000001"][0, :]


# image cleaning
def clean_eiger_image(img_raw):
    img_clean = np.copy(img_raw)
    img_clean[np.isnan(img_clean)] = 0
    img_clean[img_clean == 4294967295] = 0
    return img_clean


def analyze_xrd_scan(save_folder, logger):
    for master_file_name in glob.glob(f"{save_folder}/*_master.h5"):
        logger.info(f"Analyzing {master_file_name}")
        save_file_name = os.path.basename(master_file_name).replace(
            "_master.h5", "_image.png"
        )
        save_file_pathname = os.path.join(save_folder, save_file_name)

        img_clean = clean_eiger_image(load_eiger_image_h5(master_file_name))

        # additional preprocess
        img_preped = preprocess_image(img_clean)

        # plot image
        plt.figure()
        # plt.imshow(img_preped, vmin=0, vmax=25, cmap=colormap)
        plt.imshow(np.log(img_preped + 1), cmap=colormap)
        plt.savefig(save_file_pathname)

        # azimuthally integrate
        integrated_I, tth_bin_edges, tth_bin_centers = azimuthal_integration(
            img_preped, detector, cake_params
        )

        # plot intengrated I
        plt.figure()
        plt.plot(tth_bin_centers[40:], integrated_I[40:])
        plt.xlabel(r"$2 \theta$")
        plt.ylabel(
            r"$I$",
        )
        plt.savefig(save_file_pathname.replace("_image.png", "_integrated.png"))


class XRDAnalysis:
    def __init__(self, dagster_context, folder_id, girder_connection):
        self.context = dagster_context
        self.folder_id = folder_id
        self.girder = girder_connection
        self._items = None

    @property
    def items(self):
        if self._items is None:
            self._items = {}
            for _ in self.girder.list_item(self.folder_id):
                if _["name"].endswith("_master.h5"):
                    prefix = _["name"].split("_master")[0]
                elif "_data_" in _["name"]:
                    prefix = _["name"].split("_data_")[0]

                if prefix not in self._items:
                    self._items[prefix] = {"master": None, "data": []}

                if _["name"].endswith("_master.h5"):
                    self._items[prefix]["master"] = _
                else:
                    self._items[prefix]["data"].append(_)
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
                for png_file in glob.glob(os.path.join(tmpdir, "*.png")):
                    self.context.log.info(f"Uploading {os.path.basename(png_file)}")
                    png_upload = self.girder.upload_file_to_folder(
                        item["master"]["folderId"],
                        png_file,
                        mime_type="image/png",
                        filename=os.path.basename(png_file),
                    )
                    metadata = {
                        "runId": self.context.run.run_id,
                        "dataflowId": os.environ.get("DATAFLOW_ID", "unknown"),
                        "specId": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
                        "prov": {
                            "wasDerivedFrom": item["master"]["_id"],
                            "wasGeneratedBy": self.version,
                        },
                    }
                    if png_upload is not None:
                        self.girder._client.addMetadataToItem(png_upload["itemId"], metadata)
                        outputs.append(png_upload["itemId"])
                    else:
                        self.context.log.error(f"Failed to upload {png_file}")

            self.girder._client.addMetadataToItem(
                item["master"]["_id"], {"prov": {"hadDerivation": outputs}}
            )
