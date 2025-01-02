import abc
from spaceone.core.connector import BaseConnector


class FileBaseConnector(BaseConnector):
    
    @abc.abstractmethod
    def get_upload_url(self, file_id: str, file_name: str) -> [str, dict]:
        pass

    @abc.abstractmethod
    def get_download_url(self, file_id: str, file_name: str) -> str:
        pass

    @abc.abstractmethod
    def check_file(self, remote_file_path:str) -> bool:
        pass

    @abc.abstractmethod
    def delete_file(self, remote_file_path:str ) -> None:
        pass

    @abc.abstractmethod
    def upload_file(self, remote_file_path: str, data:bytes) -> None:
        pass
    
    @abc.abstractmethod
    def download_file(self, remote_file_path: str ):
        pass
    