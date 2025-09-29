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

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:

        if hasattr(self.file_conn, 'stream_upload_file'):
            self.file_conn.stream_upload_file(resource_group, file_id, file_obj)
        else:
            # 스트리밍을 지원하지 않는 커넥터의 경우 기존 방식으로 폴백
            _LOGGER.warning(f"[stream_upload_file] Connector {type(self.file_conn).__name__} does not support streaming, falling back to regular upload")
            # 청크 단위로 읽어서 메모리 사용량 최적화
            file_data = b""
            chunk_size = 8192  # 8KB 청크
            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break
                file_data += chunk
            self.file_conn.upload_file(resource_group, file_id, file_data)

    def download_file(self, resource_group:str, file_id:str ) :
        return self.file_conn.download_file(resource_group, file_id)
