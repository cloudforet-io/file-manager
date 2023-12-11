import logging

from spaceone.core import config, cache
from spaceone.core.manager import BaseManager
from spaceone.file_manager.error import *
from spaceone.file_manager.connector.file_base_connector import FileBaseConnector

_LOGGER = logging.getLogger(__name__)


class PublicFileConnectorManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        backend = config.get_global("BACKEND", "PublicFileConnectorManager")

        try:
            _LOGGER.debug(f"[PublicFileConnectorManager] Create {backend}")
            self.file_conn: FileBaseConnector = self.locator.get_connector(backend)
        except Exception as e:
            _LOGGER.error(f"[PublicFileConnectorManager] not defined backend {backend}")
            raise ERROR_NOT_DEFINED_FILE_BACKEND(backend=backend)

    def get_upload_url(self, public_file_id, public_file_name):
        upload_url, upload_options = self.file_conn.get_upload_url(
            public_file_id, public_file_name
        )
        return upload_url, upload_options

    @cache.cacheable(key="file-manager:download-url:{public_file_id}", expire=1800)
    def get_download_url(self, public_file_id: str, public_file_name: str):
        download_url = self.file_conn.get_download_url(public_file_id, public_file_name)
        return download_url

    def check_file(self, public_file_id: str, public_file_name: str):
        return self.file_conn.check_file(public_file_id, public_file_name)

    def delete_file(self, public_file_id: str, public_file_name: str) -> None:
        self.file_conn.delete_file(public_file_id, public_file_name)
