import logging

from spaceone.core import config, cache
from spaceone.core.manager import BaseManager
from spaceone.file_manager.error import *
from spaceone.file_manager.connector.file_base_connector import FileBaseConnector

_LOGGER = logging.getLogger(__name__)


class FileConnectorManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        backend = config.get_global("BACKEND", "FileConnectorManager")
        try:
            _LOGGER.debug(f"[FileConnectorManager] Create {backend}")
            self.file_conn: FileBaseConnector = self.locator.get_connector(backend)
        except Exception as e:
            _LOGGER.error(f"[FileConnectorManager] not defined backend {backend}")
            raise ERROR_NOT_DEFINED_FILE_BACKEND(backend=backend)

    def check_file(self, resource_group:str, file_id:str ):
        return self.file_conn.check_file(resource_group, file_id)

    def delete_file(self, resource_group:str, file_id:str ) -> None:
        self.file_conn.delete_file(resource_group, file_id)

    def upload_file(self, resource_group:str, file_id:str , file_binary: bytes) -> None:
        self.file_conn.upload_file( resource_group, file_id, file_binary)  

    def download_file(self, resource_group:str, file_id:str ) :
        return self.file_conn.download_file(resource_group, file_id)
