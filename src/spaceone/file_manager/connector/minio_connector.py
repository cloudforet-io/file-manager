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
        
    def get_upload_url(self, file_id, file_name):
        object_name = self._generate_object_name(file_id, file_name)
        response = self.client.generate_presigned_post(self.bucket_name, object_name)
        return response["url"], response["fields"]
    
    def get_download_url(self, file_id: str, file_name: str) -> str:
        object_name = self._generate_object_name(file_id, file_name)
        response = self.client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket_name, "Key": object_name}, ExpiresIn=86400
        )
        return response
    
    def check_file(self, file_id, file_name):
        object_name = self._generate_object_name(file_id, file_name)
        try:
            self.client.get_object(Bucket=self.bucket_name, Key=object_name)
            return True
        except botocore.exceptions.ClientError as e:
            _LOGGER.debug(f"[check_file] get_object error: {e}")
            return False
    
    def delete_file(self, file_id, file_name):
        object_name = self._generate_object_name(file_id, file_name)
        self.client.delete_object(Bucket=self.bucket_name, Key=object_name)

    @staticmethod
    def _generate_object_name(file_id, file_name):
        return f"{file_id}/{file_name}"

    def upload_file(self, remote_file_path:str, data: bytes) -> None:
        file_obj =  BytesIO(data)
        if self.client is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        self.client.upload_fileobj(file_obj, self.bucket_name, remote_file_path)
    
    def download_file(self, remote_file_path:str) :
        
        if self.client is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")

        obj = self.client.get_object(Bucket=self.bucket_name, Key=remote_file_path)
        return obj["Body"]