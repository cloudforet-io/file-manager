import logging
import boto3
from io import BytesIO
import botocore
from botocore.client import Config as BotocoreConfig

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
        endpoint = self.config.get("minio_endpoint")
        access_key_id = self.config.get("minio_access_key_id")
        secret_access_key = self.config.get("minio_secret_access_key")
        region_name =  self.config.get("region_name")
        
        if endpoint is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        if region_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        
        if access_key_id and secret_access_key:
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name=region_name,
                config=BotocoreConfig(signature_version='s3v4'),
            )
        else:
            self.client = boto3.client("s3", region_name=region_name)
            
    def _set_bucket(self):
        bucket_name = self.config.get("bucket_name")
        
        if bucket_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        self.bucket_name = bucket_name
        
    def check_file(self, resource_group, file_id):
        
        object_name = self._generate_object_name(resource_group, file_id)
        try:
            self.client.get_object(Bucket=self.bucket_name, Key=object_name)
            return True
        except botocore.exceptions.ClientError as e:
            _LOGGER.debug(f"[check_file] get_object error: {e}")
            return False
    
    def delete_file(self, resource_group, file_id):
        object_name = self._generate_object_name(resource_group, file_id)
        self.client.delete_object(Bucket=self.bucket_name, Key=object_name)

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


    def upload_file(self, resource_group:str, file_id:str, data: bytes) -> None:
        object_name = self._generate_object_name(resource_group, file_id)
        file_obj =  BytesIO(data)
        if self.client is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        self.client.upload_fileobj(file_obj, self.bucket_name, object_name)
    
    def download_file(self, resource_group:str, file_id:str) :
        
        object_name = self._generate_object_name(resource_group, file_id)
        if self.client is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")

        obj = self.client.get_object(Bucket=self.bucket_name, Key=object_name)
        return obj["Body"]