import glob
import os
import tempfile
from importlib.metadata import version

import fabio
import matplotlib.pyplot as plt
from pyFAI.detectors import Eiger2CdTe_1M
from pyFAI.integrator.azimuthal import AzimuthalIntegrator


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


class XRDAnalysis:
    def __init__(self, dagster_context, folder_ids, girder_connection):
        self.context = dagster_context
        self.folder_ids = folder_ids
        self.girder = girder_connection
        self._items = None

    @property
    def items(self):
        if self._items is None:
            self._items = {}
            for folder_id in self.folder_ids:
                for raw_data_folder in self.girder.list_folder(folder_id, name="raw"):
                    for _ in self.girder.list_item(raw_data_folder["_id"]):
                        if not _["name"].endswith(".h5"):
                            continue
                        if _["name"].endswith("_master.h5"):
                            prefix = _["name"].split("_master")[0]
                        elif "_data_" in _["name"]:
                            prefix = _["name"].split("_data_")[0]
                        else:
                            continue

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
                    }
                    if output_upload is not None:
                        self.girder._client.addMetadataToItem(
                            output_upload["itemId"], metadata
                        )
                        outputs.append(output_upload["itemId"])
                    else:
                        self.context.log.error(f"Failed to upload {output_file}")

            self.girder._client.addMetadataToItem(
                item["master"]["_id"], {"prov": {"hadDerivation": outputs}}
            )
