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
        """
        S3 파일 존재 여부 확인 (타임아웃 설정)
        """
        try:
            object_name = self._generate_object_name(resource_group, file_id)
            # ✅ head_object는 자동으로 타임아웃 설정됨 (boto3 기본값)
            self.client.head_object(Bucket=self.bucket_name, Key=object_name)
            _LOGGER.debug(f"[check_file] File {object_name} exists")
            return True
        except self.client.exceptions.NoSuchKey:
            # 파일이 없는 경우
            _LOGGER.debug(f"[check_file] File not found: {object_name}")
            return False
        except Exception as e:
            _LOGGER.debug(f"[check_file] Error checking file: {e}")
            return False

    def delete_file(self, resource_group:str, file_id:str):
        object_name = self._generate_object_name(resource_group, file_id)
        self.client.delete_object(Bucket=self.bucket_name, Key=object_name)

    def upload_file(self, resource_group:str, file_id: str, data: bytes) -> None:
        """
        S3 파일 업로드 (예외 전파)
        """
        object_name = self._generate_object_name(resource_group, file_id)

        file_obj = None
        try:
            file_obj = BytesIO(data)
            _LOGGER.info(f"[upload_file] Uploading to S3: {object_name}")
            self.client.upload_fileobj(file_obj, self.bucket_name, object_name)
            _LOGGER.info(f"[upload_file] Successfully uploaded to {object_name}")
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error uploading {object_name}: {e}')
            raise  # ✅ 예외 전파
        finally:
            if file_obj:
                file_obj.close()

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        """
        S3 스트리밍 업로드
        upload_fileobj를 사용하여 메모리 효율적으로 처리
        """
        object_name = self._generate_object_name(resource_group, file_id)

        try:
            _LOGGER.info(f"[stream_upload_file] Starting upload to S3: {object_name}")

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

            # 업로드 시간 측정
            start_time = time.time()

            # boto3 client의 upload_fileobj는 자동으로 스트리밍 처리 (메모리 효율적)
            # Callback을 사용하여 진행률 로깅
            callback = self._create_progress_callback(object_name)

            content_type = getattr(file_obj, 'content_type', 'application/octet-stream')
            if content_type is None:
                content_type = 'application/octet-stream'

            self.client.upload_fileobj(
                file_stream,
                self.bucket_name,
                object_name,
                ExtraArgs={'ContentType': content_type},
                Callback=callback
            )

            upload_time = time.time() - start_time
            _LOGGER.info(f"[stream_upload_file] Upload completed in {upload_time:.2f}s")

        except Exception as e:
            _LOGGER.error(f'[stream_upload_file] Error: {e}')
            raise e

    def _create_progress_callback(self, object_name: str):
        """
        진행률 콜백 함수 생성
        """
        class ProgressCallback:
            def __init__(self, object_name: str):
                self.object_name = object_name
                self.total_uploaded = 0
                self.last_logged = 0
                self.log_interval = 10 * 1024 * 1024  # 10MB마다 로깅

            def __call__(self, bytes_amount):
                self.total_uploaded += bytes_amount
                if self.total_uploaded - self.last_logged >= self.log_interval:
                    _LOGGER.info(f"[stream_upload_file] Uploaded {self.total_uploaded // (1024*1024)}MB to {self.object_name}")
                    self.last_logged = self.total_uploaded

        return ProgressCallback(object_name)

    def download_file(self, resource_group:str, file_id: str):
        """
        S3 파일 다운로드 (스트림 관리 개선)
        """
        object_name = self._generate_object_name(resource_group, file_id)

        try:
            _LOGGER.info(f"[download_file] Downloading from S3: {object_name}")
            obj = self.client.get_object(Bucket=self.bucket_name, Key=object_name)

            # ✅ 메타데이터 검증
            if 'ContentLength' not in obj:
                if 'Body' in obj:
                    obj['Body'].close()
                raise ValueError(f"Missing ContentLength for {object_name}")

            # ✅ 파일 크기 제한 (안전장치)
            max_size = 5 * 1024 * 1024 * 1024  # 5GB 제한
            if obj['ContentLength'] > max_size:
                obj['Body'].close()
                raise ValueError(f"File too large: {obj['ContentLength']} bytes > {max_size} bytes")

            _LOGGER.info(f"[download_file] File size: {obj['ContentLength'] // (1024*1024)}MB")
            return obj

        except Exception as e:
            _LOGGER.error(f'[download_file] Error downloading {object_name}: {e}')
            raise


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
