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
        """
        MinIO 파일 존재 여부 확인
        """
        object_name = self._generate_object_name(resource_group, file_id)
        try:
            self.client.stat_object(self.bucket_name, object_name)
            _LOGGER.debug(f"[check_file] File {object_name} exists")
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey' or e.code == 'NoSuchBucket':
                _LOGGER.debug(f"[check_file] File not found: {object_name}")
                return False
            _LOGGER.debug(f"[check_file] Error checking file: {e}")
            return False
        except Exception as e:
            _LOGGER.debug(f"[check_file] Unexpected error: {e}")
            return False

    def delete_file(self, resource_group, file_id):
        object_name = self._generate_object_name(resource_group, file_id)
        try:
            self.client.remove_object(self.bucket_name, object_name)
        except S3Error as e:
            _LOGGER.debug(f"[delete_file] remove_object error: {e}")

    def upload_file(self, resource_group:str, file_id:str, data: bytes) -> None:
        """
        MinIO 파일 업로드 (예외 전파)
        """
        object_name = self._generate_object_name(resource_group, file_id)

        data_stream = None
        try:
            data_stream = BytesIO(data)
            data_length = data_stream.getbuffer().nbytes
            _LOGGER.info(f"[upload_file] Uploading to MinIO: {object_name}")
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=data_stream,
                length=data_length
            )
            _LOGGER.info(f"[upload_file] Successfully uploaded to {object_name}")
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error uploading {object_name}: {e}')
            raise  # ✅ 예외 전파
        finally:
            if data_stream:
                data_stream.close()

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        """
        MinIO 스트리밍 업로드
        put_object를 사용하여 메모리 효율적으로 처리
        """
        object_name = self._generate_object_name(resource_group, file_id)

        try:
            _LOGGER.info(f"[stream_upload_file] Starting upload to MinIO: {object_name}")

            # 파일 객체 타입에 따른 처리
            if hasattr(file_obj, 'file'):
                # FastAPI UploadFile 객체의 경우
                _LOGGER.debug(f"[stream_upload_file] Detected FastAPI UploadFile object")
                file_stream = file_obj.file
                content_type = getattr(file_obj, 'content_type', 'application/octet-stream')
            else:
                # 일반 파일 객체의 경우
                _LOGGER.debug(f"[stream_upload_file] Detected standard file object")
                file_stream = file_obj
                content_type = 'application/octet-stream'

            # 파일 크기 계산
            current_pos = file_stream.tell() if hasattr(file_stream, 'tell') else 0
            if hasattr(file_stream, 'seek'):
                file_stream.seek(0, 2)  # 파일 끝으로 이동
                file_size = file_stream.tell()
                file_stream.seek(current_pos)  # 원래 위치로 복원
            else:
                # 크기를 모를 경우 -1 (MinIO가 처리)
                file_size = -1

            # 업로드 시간 측정
            start_time = time.time()

            # MinIO put_object는 자동으로 스트리밍 처리
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=file_stream,
                length=file_size,
                content_type=content_type
            )

            upload_time = time.time() - start_time
            if file_size > 0:
                _LOGGER.info(f"[stream_upload_file] Upload completed. Size: {file_size // (1024*1024)}MB, Time: {upload_time:.2f}s")
            else:
                _LOGGER.info(f"[stream_upload_file] Upload completed in {upload_time:.2f}s")

        except Exception as e:
            _LOGGER.error(f'[stream_upload_file] Error: {e}')
            raise e

    def download_file(self, resource_group:str, file_id:str):
        """
        MinIO 파일 다운로드 (스트림 관리 개선)
        """
        obj = None
        try:
            object_name = self._generate_object_name(resource_group, file_id)
            _LOGGER.info(f"[download_file] Downloading from MinIO: {object_name}")

            obj = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )

            # ✅ 스트림 정보 검증
            if not hasattr(obj, 'read'):
                raise ValueError(f"Invalid object stream for {object_name}")

            # ✅ 메타데이터 가져오기 (전체 읽기하지 않음)
            try:
                stat = self.client.stat_object(
                    bucket_name=self.bucket_name,
                    object_name=object_name
                )
                content_length = stat.size
            except Exception as stat_error:
                _LOGGER.warning(f"[download_file] Cannot get file size: {stat_error}")
                content_length = -1

            _LOGGER.info(f"[download_file] File size: {content_length // (1024*1024)}MB" if content_length > 0 else f"[download_file] Streaming object")

            # ✅ 스트림을 그대로 반환 (메모리 효율적)
            return {
                'Body': obj,
                'ContentLength': content_length
            }

        except Exception as e:
            _LOGGER.error(f'[download_file] Error downloading: {e}')
            # ✅ 에러 시 스트림 정리
            if obj:
                try:
                    obj.close()
                except:
                    pass
                try:
                    obj.release_conn()
                except:
                    pass
            raise  # ✅ 예외 전파 (None 반환 대신)


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
