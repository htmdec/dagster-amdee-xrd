import json
import threading
from contextlib import contextmanager

import requests
from dagster import (
    ConfigurableResource,
)
from girder_client import GirderClient
from pydantic import PrivateAttr


class GirderClientWithSession(GirderClient):
    def __init__(
        self,
        host=None,
        port=None,
        apiRoot=None,
        scheme=None,
        apiUrl=None,
        apiKey=None,
        token=None,
        session=None,
        cacheSettings=None,
        progressReporterCls=None,
    ):
        super().__init__(
            host=host,
            port=port,
            apiRoot=apiRoot,
            scheme=scheme,
            apiUrl=apiUrl,
            cacheSettings=cacheSettings,
            progressReporterCls=progressReporterCls,
        )

        if token:
            self.setToken(token)

        if apiKey:
            self.authenticate(apiKey=apiKey)

        self._session = session


_girder_client_cache: dict[tuple, "GirderClientWithSession"] = {}
_girder_client_cache_lock = threading.Lock()

_FOLDER_CACHE_MAX = 5000
_folder_cache: dict[str, dict] = {}
_folder_cache_lock = threading.Lock()


class GirderCredentials(ConfigurableResource):
    api_url: str
    api_key: str


class GirderConnection(ConfigurableResource):
    credentials: GirderCredentials
    _client: GirderClientWithSession = PrivateAttr()

    def _make_client(self):
        session = requests.Session()
        return GirderClientWithSession(
            apiUrl=self.credentials.api_url,
            apiKey=self.credentials.api_key,
            session=session,
        )

    @contextmanager
    def yield_for_execution(self, context):
        key = (self.credentials.api_url, self.credentials.api_key)
        with _girder_client_cache_lock:
            client = _girder_client_cache.get(key)
            if client is None or client.get("user/me") is None:
                _girder_client_cache[key] = self._make_client()
        self._client = _girder_client_cache[key]
        yield self

    @property
    def client(self):
        if not self._client:
            raise Exception(
                "Girder client is not initialized. Use yield_for_execution."
            )
        return self._client

    def list_folders(self, parent_id, parent_type="folder", name=None):
        params = {
            "parentId": parent_id,
            "parentType": parent_type,
            "sort": "created",
            "sortdir": -1,
        }
        if name:
            params["name"] = name
        return self._client.listResource("folder", params, limit=None, offset=None)

    def list_item(
        self, folder_id, name=None, limit=None, sort=None, sortdir=1, offset=None
    ):
        params = {"folderId": folder_id, "sortdir": sortdir}
        if name:
            params["name"] = name
        if sort:
            params["sort"] = sort
        return self._client.listResource("item", params, limit=limit, offset=offset)

    def query_items(self, query, sort="_id", sortdir=1, page_size=500):
        """Yield items matching a MongoDB query via Girder's ``item/query`` endpoint.

        Pages manually (rather than via ``listResource``) so we can use a page
        size larger than girder_client's default of 50.
        """
        params = {
            "query": json.dumps(query),
            "sort": sort,
            "sortdir": sortdir,
            "limit": page_size,
            "offset": 0,
        }
        while True:
            records = self._client.get("item/query", params)
            yield from records
            if len(records) < page_size:
                break
            params["offset"] += len(records)

    def get_folder(self, folder_id):
        """Return a folder document, memoized process-wide.

        Folder names and parents are effectively immutable here, so caching
        keeps repeated sensor ticks from re-fetching the same ancestors.
        """
        folder = _folder_cache.get(folder_id)
        if folder is None:
            folder = self._client.getFolder(folder_id)
            with _folder_cache_lock:
                if len(_folder_cache) >= _FOLDER_CACHE_MAX:
                    _folder_cache.clear()
                _folder_cache[folder_id] = folder
        return folder

    def get_user(self):
        return self._client.get("user/me")
