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
    def check_file(self, file_id: str, file_name: str) -> bool:
        pass

    @abc.abstractmethod
    def delete_file(self, file_id: str, file_name: str) -> None:
        pass
