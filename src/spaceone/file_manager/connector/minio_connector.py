import logging
import time
from math import log
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
        
        if endpoint is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        if access_key_id and secret_access_key:
            self.client = Minio(
                endpoint=endpoint,
                access_key=access_key_id,
                secret_key=secret_access_key,
                secure=False
            )
        else:
            self.client = Minio(
                endpoint=endpoint,
                secure=False
            )
            
    def _set_bucket(self):
        bucket_name = self.config.get("bucket_name")
        
        if bucket_name is None:
            raise ERROR_CONNECTOR_CONFIGURATION(backend="MinIOS3Connector")
        
        self.bucket_name = bucket_name
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            logging.info(f"Bucket {bucket_name} created")
        
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
            data_stream = BytesIO(data)
            data_length = data_stream.getbuffer().nbytes  # Get the size of the data in bytes
            self.client.put_object(self.bucket_name, object_name,  data_stream, data_length)
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
        finally:
            data_stream.close()

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        object_name = self._generate_object_name(resource_group, file_id)

        try:
            _LOGGER.info(f"[stream_upload_file] Starting upload to MinIO: {object_name}")
            chunk_size = 8 * 1024 * 1024  # 8MB

            if hasattr(file_obj, 'file'):
                # FastAPI UploadFile 객체의 경우
                _LOGGER.debug(f"[stream_upload_file] Detected FastAPI UploadFile object")

                # 임시 파일로 스트리밍 처리
                stream_buffer = BytesIO()
                total_size = 0

                # 청크 단위로 읽어서 MinIO에 업로드
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
                data_length = stream_buffer.getbuffer().nbytes

                # 업로드 시간 측정
                start_time = time.time()
                self.client.put_object(
                    self.bucket_name,
                    object_name,
                    stream_buffer,
                    data_length,
                    content_type=getattr(file_obj, 'content_type', 'application/octet-stream')
                )
                upload_time = time.time() - start_time
                stream_buffer.close()
                _LOGGER.info(f"[stream_upload_file] Upload completed. Size: {total_size // (1024*1024)}MB, Time: {upload_time:.2f}s")
            else:
                # 일반 파일 객체의 경우
                _LOGGER.debug(f"[stream_upload_file] Detected standard file object")

                # 파일 크기 확인
                current_pos = file_obj.tell()
                file_obj.seek(0, 2)  # 파일 끝으로 이동
                file_size = file_obj.tell()
                file_obj.seek(current_pos)  # 원래 위치로 복원

                start_time = time.time()
                self.client.put_object(
                    self.bucket_name,
                    object_name,
                    file_obj,
                    file_size
                )
                upload_time = time.time() - start_time
                _LOGGER.info(f"[stream_upload_file] Upload completed in {upload_time:.2f}s")
        except Exception as e:
            _LOGGER.error(f'[stream_upload_file] Error: {e}')
            raise e

    def download_file(self, resource_group:str, file_id:str) :
        try:
            object_name = self._generate_object_name(resource_group, file_id)
            obj = self.client.get_object(bucket_name=self.bucket_name, object_name=object_name)
            data = {}
            data_stream = BytesIO(obj.read())
            data['Body'] = data_stream
            data['ContentLength'] = data_stream.getbuffer().nbytes
            return data
        except Exception as e:
            _LOGGER.error(f'[download_file] Error: {e}')
            return None
        

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