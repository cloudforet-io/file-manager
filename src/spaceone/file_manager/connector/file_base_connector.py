import abc
from spaceone.core.connector import BaseConnector


class FileBaseConnector(BaseConnector):
    
    @abc.abstractmethod
    def check_file(self, resource_group:str, file_id:str) -> bool:
        pass

    @abc.abstractmethod
    def delete_file(self, resource_group:str, file_id:str ) -> None:
        pass

    @abc.abstractmethod
    def upload_file(self, resource_group:str, file_id:str, data:bytes) -> None:
        pass

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        pass

    @abc.abstractmethod
    def download_file(self, resource_group:str, file_id:str ):
        pass
    