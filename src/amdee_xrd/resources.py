import io
import os
import urllib.parse as parse
from contextlib import contextmanager

import girder_client
from dagster import (
    ConfigurableIOManagerFactory,
    ConfigurableResource,
    InputContext,
    IOManager,
    MetadataValue,
    OutputContext,
)
from girder_client import GirderClient
from pydantic import PrivateAttr


class GirderCredentials(ConfigurableResource):
    api_url: str
    api_key: str


class GirderConnection(ConfigurableResource):
    credentials: GirderCredentials
    _client: girder_client.GirderClient = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context):
        self._client = girder_client.GirderClient(apiUrl=self.credentials.api_url)
        self._client.authenticate(apiKey=self.credentials.api_key)
        yield self

    def list_folder(self, folder_id):
        return list(self._client.listFolder(folder_id))

    def folder_by_name(self, parentId, name):
        folders = self._client.get(
            "folder",
            parameters={"name": name, "parentType": "folder", "parentId": parentId},
        )
        if not folders:
            return None
        return folders[0]

    def master_files(self, folder_id):
        items = self.list_item(folder_id)
        return [item for item in items if item["name"].endswith("_master.h5")]

    def folder_details(self, folder_id):
        return self._client.get(f"/folder/{folder_id}/details")

    def list_item(self, folder_id):
        return list(self._client.listItem(folder_id))

    def get_item(self, item_id):
        return self._client.getItem(item_id)

    def get_file_from_item(self, item_id):
        files = self._client.get(
            f"item/{item_id}/files",
            parameters={"limit": 1, "offset": 0, "sort": "created", "sortdir": -1},
        )
        return files[0]

    def get_stream(self, item_id):
        fobj = self.get_file_from_item(item_id)
        data = io.BytesIO()
        self._client.downloadFile(fobj["_id"], data)
        data.seek(0)
        return data

    def upload_file_to_folder(
        self, folder_id, file_path, mime_type=None, filename=None
    ):
        return self._client.uploadFileToFolder(
            folder_id, file_path, mimeType=mime_type, filename=filename
        )

    def get_user(self):
        return self._client.get("user/me")


class GirderIOManager(IOManager):
    def __init__(self, api_url, api_key, source_folder_id, target_folder_id):
        self._cli = GirderClient(apiUrl=api_url)
        self._cli.authenticate(apiKey=api_key)
        self.source_folder_id = source_folder_id
        self.target_folder_id = target_folder_id

    def _get_path(self, context) -> str:
        if context.has_partition_key:
            return "/".join(context.asset_key.path + [context.asset_partition_key])
        else:
            return "/".join(context.asset_key.path)

    def handle_output(self, context: OutputContext, obj):
        if not obj:
            return
        item, file = self._get_file(context, self.target_folder_id, suffix="png")
        size = obj.seek(0, os.SEEK_END)
        obj.seek(0)

        if file:
            fobj = self._cli.uploadFileContents(file["_id"], obj, size)
        else:
            file = self._cli.post(
                "file",
                parameters={
                    "parentType": "item",
                    "parentId": item["_id"],
                    "name": item["name"],
                    "size": size,
                    "mimeType": "image/png",
                },
            )
            fobj = self._cli._uploadContents(file, obj, size)
        girder_metadata = {
            "code_version": context.version,
            "run_id": context.run_id,
            "dataflow": os.environ.get("DATAFLOW_ID", "unknown"),
            "spec": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
        }
        self._cli.addMetadataToItem(item["_id"], girder_metadata)
        girder_url = parse.urlparse(self._cli.urlBase)
        metadata = {
            "size": fobj["size"],
            "item_url": MetadataValue.url(
                f"{girder_url.scheme}://{girder_url.netloc}/#item/{fobj['itemId']}"
            ),
            "download_url": MetadataValue.url(
                f"{self._cli.urlBase}file/{fobj['_id']}/download"
            ),
            "dataflow": os.environ.get("DATAFLOW_ID", "unknown"),
            "spec": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
            "docker_image": os.environ.get("DAGSTER_CURRENT_IMAGE", "unknown"),
        }
        if context.has_asset_key:
            context.add_output_metadata(metadata)

    def load_input(self, context: InputContext):
        item, file = self._get_file(context, self.source_folder_id)
        if not file:
            raise Exception(
                f"Expected to find exactly one file at path {item['name']}."
            )

        fp = io.BytesIO()
        self._cli.downloadFile(file["_id"], fp)
        fp.seek(0)
        return fp

    def _get_file(self, context, folder_id, suffix=None):
        path = self._get_path(context)
        name = ".".join(os.path.basename(path).rsplit("_", 1))
        if suffix:
            name = name.replace("csv", suffix)
        item = self._cli.loadOrCreateItem(name, folder_id)

        files = self._cli.get(
            f"item/{item['_id']}/files",
            parameters={"limit": 1, "offset": 0, "sort": "created", "sortdir": -1},
        )
        try:
            file = files[0]
        except (TypeError, IndexError):
            file = None
        return item, file


class ConfigurableGirderIOManager(ConfigurableIOManagerFactory):
    api_key: str
    api_url: str
    source_folder_id: str
    target_folder_id: str

    def create_io_manager(self, context) -> GirderIOManager:
        return GirderIOManager(
            self.api_url, self.api_key, self.source_folder_id, self.target_folder_id
        )
