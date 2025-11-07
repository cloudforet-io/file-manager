import logging
import boto3
import time
from io import BytesIO
import botocore

from spaceone.core.error import *
from spaceone.file_manager.connector.file_base_connector import FileBaseConnector

__all__ = ["AWSS3Connector"]
_LOGGER = logging.getLogger(__name__)


class AWSS3Connector(FileBaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = None
        self.bucket_name = None
        self._create_session()
        self._set_bucket()

    def _create_session(self):
        aws_access_key_id = self.config.get("aws_access_key_id")
        aws_secret_access_key = self.config.get("aws_secret_access_key")
        region_name = self.config.get("region_name")

        if region_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="AWSS3Connector")

        if aws_access_key_id and aws_secret_access_key:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )
        else:
            self.client = boto3.client("s3", region_name=region_name)

    def _set_bucket(self):
        bucket_name = self.config.get("bucket_name")

        if bucket_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="AWSS3Connector")

        self.bucket_name = bucket_name

    def check_file(self, resource_group:str, file_id:str ):
        try:
            object_name = self._generate_object_name(resource_group, file_id)
            self.client.head_object(Bucket=self.bucket_name, Key=object_name)
            return True
        except Exception as e:
            _LOGGER.debug(f"[check_file] get_object error: {e}")
            return False

    def delete_file(self, resource_group:str, file_id:str):
        object_name = self._generate_object_name(resource_group, file_id)
        self.client.delete_object(Bucket=self.bucket_name, Key=object_name)

    def upload_file(self, resource_group:str, file_id: str, data: bytes) -> None:
        object_name = self._generate_object_name(resource_group, file_id)
        
        try:
            file_obj =  BytesIO(data)
            self.client.upload_fileobj(file_obj, self.bucket_name, object_name)
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
        finally:
            file_obj.close()

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        object_name = self._generate_object_name(resource_group, file_id)

        try:
            _LOGGER.info(f"[stream_upload_file] Starting upload to S3: {object_name}")

            # 청크 사이즈 설정 (8MB - S3에서 권장하는 청크 사이즈)
            chunk_size = 8 * 1024 * 1024  # 8MB

            # 파일 객체 타입에 따른 처리
            if hasattr(file_obj, 'file'):
                # FastAPI UploadFile 객체의 경우
                _LOGGER.debug(f"[stream_upload_file] Detected FastAPI UploadFile object")

                # 임시 파일로 스트리밍 처리
                stream_buffer = BytesIO()
                total_size = 0

                # 청크 단위로 읽어서 S3에 업로드
                while True:
                    chunk = file_obj.file.read(chunk_size)
                    if not chunk:
                        break
                    stream_buffer.write(chunk)
                    total_size += len(chunk)

                    # 진행률 로깅 (10MB마다)
                    if total_size % (10 * 1024 * 1024) == 0 and total_size > 0:
                        _LOGGER.info(f"[stream_upload_file] Uploaded {total_size // (1024*1024)}MB")

                # 스트림 버퍼를 처음으로 리셋하고 업로드
                stream_buffer.seek(0)

                # 업로드 시간 측정
                start_time = time.time()

                content_type = getattr(file_obj, 'content_type', 'application/octet-stream')
                if content_type is None:
                    content_type = 'application/octet-stream'

                self.client.upload_fileobj(
                    stream_buffer,
                    self.bucket_name,
                    object_name,
                    ExtraArgs={'ContentType': content_type}
                )
                upload_time = time.time() - start_time
                stream_buffer.close()
                _LOGGER.info(f"[stream_upload_file] Upload completed. Size: {total_size // (1024*1024)}MB, Time: {upload_time:.2f}s")
            else:
                # 일반 파일 객체의 경우
                _LOGGER.debug(f"[stream_upload_file] Detected standard file object")
                start_time = time.time()
                self.client.upload_fileobj(file_obj, self.bucket_name, object_name)
                upload_time = time.time() - start_time
                _LOGGER.info(f"[stream_upload_file] Upload completed in {upload_time:.2f}s")
        except Exception as e:
            _LOGGER.error(f'[stream_upload_file] Error: {e}')
            raise e

    def download_file(self, resource_group:str, file_id: str) :
        
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