import logging
from minio import Minio 
from minio.error import S3Error
from io import BytesIO

from spaceone.core.error import *
from spaceone.file_manager.connector.file_base_connector import FileBaseConnector

__all__ = ["MinIOS3Connector"]
_LOGGER = logging.getLogger(__name__)

class MinIOS3Connector(FileBaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = None
        self.bucket_name = None
        self._create_session()
        self._set_bucket()
        
    def _create_session(self):
        endpoint = self.config.get("endpoint")
        access_key_id = self.config.get("minio_access_key_id")
        secret_access_key = self.config.get("minio_secret_access_key")
        region_name =  self.config.get("region_name")
        
        if endpoint is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        if region_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        
        if access_key_id and secret_access_key:
            self.client = Minio(
                endpoint=endpoint,
                access_key=access_key_id,
                secret_key=secret_access_key,
                region = region_name,
                secure=True
            )
        else:
            self.client = Minio(
                endpoint=endpoint,
                secure=True
            )
            
    def _set_bucket(self):
        bucket_name = self.config.get("bucket_name")
        
        if bucket_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        self.bucket_name = bucket_name
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
        
    def check_file(self, resource_group, file_id):
        
        object_name = self._generate_object_name(resource_group, file_id)
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as e:
            _LOGGER.debug(f"[check_file] get_object error: {e}")
            return False
    
    def delete_file(self, resource_group, file_id):
        object_name = self._generate_object_name(resource_group, file_id)
        try:
            self.client.remove_object(self.bucket_name, object_name)
        except S3Error as e:
            _LOGGER.debug(f"[delete_file] remove_object error: {e}")

    def upload_file(self, resource_group:str, file_id:str, data: bytes) -> None:
        object_name = self._generate_object_name(resource_group, file_id)
        
        try:
            file_obj =  BytesIO(data)
            self.client.put_object(file_obj, self.bucket_name, object_name)
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
        finally:
            file_obj.close()
    
    def download_file(self, resource_group:str, file_id:str) :
        object_name = self._generate_object_name(resource_group, file_id)
        obj = self.client.get_object(Bucket=self.bucket_name, Key=object_name)
        return obj

    @staticmethod
    def _generate_object_name(resource_group:str, file_id: str):
        if resource_group == "SYSTEM":
            return f"/files/public/{file_id}"
        elif resource_group == "DOMAIN":
            return f"/files/domain/{file_id}"
        elif resource_group == "WORKSPACE":
            return f"/files/workspace/{file_id}"
        elif resource_group == "PROJECT":
            return f"/files/project/{file_id}"
        elif resource_group == "USER":
            return f"/files/user/{file_id}"