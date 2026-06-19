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

    def get_user(self):
        return self._client.get("user/me")
