import logging

from spaceone.core import config, cache
from spaceone.core.manager import BaseManager
from spaceone.file_manager.error import *
from spaceone.file_manager.connector.file_base_connector import FileBaseConnector

_LOGGER = logging.getLogger(__name__)


class FileConnectorManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        backend = config.get_global('BACKEND', 'FileConnectorManager')
        try:
            _LOGGER.debug(f'[FileConnectorManager] Create {backend}')
            self.file_conn: FileBaseConnector = self.locator.get_connector(backend)
        except Exception as e:
            _LOGGER.error(f'[FileConnectorManager] not defined backend {backend}')
            raise ERROR_NOT_DEFINED_FILE_BACKEND(backend=backend)

    def get_upload_url(self, file_id, file_name):
        upload_url, upload_options = self.file_conn.get_upload_url(file_id, file_name)
        return upload_url, upload_options

    @cache.cacheable(key='file-manager:download-url:{domain_id}:{file_id}', expire=1800)
    def get_download_url(self, file_id, file_name, domain_id):
        print('get_download_url')
        download_url = self.file_conn.get_download_url(file_id, file_name)
        return download_url

    def check_file(self, file_id, file_name):
        return self.file_conn.check_file(file_id, file_name)

    def delete_file(self, file_id, file_name):
        self.file_conn.delete_file(file_id, file_name)
