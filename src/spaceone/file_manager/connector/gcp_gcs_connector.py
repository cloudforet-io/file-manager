import logging
import json
import time
from typing import Optional
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO
import base64

from spaceone.core.error import *
from spaceone.file_manager.connector.file_base_connector import FileBaseConnector

__all__ = ["GCPGCSConnector"]
_LOGGER = logging.getLogger(__name__)


class GCPGCSConnector(FileBaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client: Optional[storage.Client] = None
        self.bucket_name: Optional[str] = None
        self._create_client()
        self._set_bucket()

    def _create_client(self):
        # GCP 인증 정보 설정
        # 1. 서비스 계정 키 JSON 문자열

        # print(f"config: {self.config}")

        service_account_key = self.config.get("service_account_key")
        decoded_key = base64.b64decode(service_account_key).decode('utf-8')

        # 2. 프로젝트 ID
        project_id = self.config.get("project_id")

        if project_id is None:
            raise Exception("GCPGCSConnector configuration error: project_id is required")
        try:
                # JSON 문자열을 딕셔너리로 파싱
                service_account_info = json.loads(decoded_key)
                # 서비스 계정 인증 정보 생성
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info
                )

                # Storage 클라이언트 생성
                self.client = storage.Client(
                    credentials=credentials,
                    project=project_id
                )
        except Exception as e:
            _LOGGER.error(f"GCS client create fail: {e}")
            _LOGGER.error(f"project_id={project_id}, has_key_json={service_account_key is not None}")
            raise Exception(f"GCPGCSConnector client creation failed: {e}")

    def _set_bucket(self):
        bucket_name = self.config.get("bucket_name")

        if bucket_name is None:
            raise Exception("GCPGCSConnector configuration error: bucket_name is required")

        self.bucket_name = bucket_name

    def check_file(self, resource_group: str, file_id: str):
        """
        GCS 파일 존재 여부 확인 (타임아웃 설정)
        """
        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")

        try:
            object_name = self._generate_object_name(resource_group, file_id)
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)

            # ✅ 파일 존재 여부 확인 (10초 타임아웃)
            exists = blob.exists(timeout=10)
            _LOGGER.debug(f"[check_file] File {object_name} exists: {exists}")
            return exists
        except Exception as e:
            _LOGGER.debug(f"[check_file] Error checking file: {e}")
            return False

    def delete_file(self, resource_group: str, file_id: str):
        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")

        try:
            object_name = self._generate_object_name(resource_group, file_id)
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)

            # 파일 삭제
            blob.delete()
        except Exception as e:
            _LOGGER.error(f"[delete_file] Error: {e}")
            raise e

    def upload_file(self, resource_group: str, file_id: str, data: bytes) -> None:

        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")

        object_name = self._generate_object_name(resource_group, file_id)
        _LOGGER.info(f"[upload_file] Starting upload to GCS: {object_name}")
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)

            # BytesIO를 사용하여 메모리에서 업로드
            file_obj = BytesIO(data)
            blob.upload_from_file(file_obj)
            _LOGGER.info(f"[upload_file] Upload completed. Size: {file_obj.getbuffer().nbytes // (1024*1024)}MB")
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
            raise e
        finally:
            file_obj.close()

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        """
        GCS 스트리밍 업로드
        upload_from_file을 사용하여 메모리 효율적으로 처리
        """
        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")

        object_name = self._generate_object_name(resource_group, file_id)

        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)

            _LOGGER.info(f"[stream_upload_file] Starting upload to GCS: {object_name}")

            # 청크 사이즈 설정 (8MB - GCS에서 권장)
            blob.chunk_size = 8 * 1024 * 1024  # 8MB

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

            # GCS 클라이언트가 자동으로 스트리밍 처리
            blob.upload_from_file(
                file_stream,
                content_type=content_type,
                timeout=600,  # 10분 타임아웃
                rewind=True   # 필요시 파일 포인터 초기화
            )

            upload_time = time.time() - start_time

            # 파일 크기 정보 로깅
            if blob.size:
                _LOGGER.info(f"[stream_upload_file] Upload completed. Size: {blob.size // (1024*1024)}MB, Time: {upload_time:.2f}s")
            else:
                _LOGGER.info(f"[stream_upload_file] Upload completed in {upload_time:.2f}s")

        except Exception as e:
            _LOGGER.error(f'[stream_upload_file] Error: {e}')
            raise e

    def download_file(self, resource_group: str, file_id: str):
        """
        GCS 파일 다운로드 (스트리밍)
        타임아웃 설정으로 무한 대기 방지
        """
        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")

        object_name = self._generate_object_name(resource_group, file_id)

        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)

            # ✅ 파일 메타데이터 새로고침 (타임아웃 설정)
            _LOGGER.info(f"[download_file] Fetching metadata for {object_name}")
            blob.reload(timeout=30)  # 30초 타임아웃

            # ✅ 파일 크기 확인
            file_size = blob.size
            if file_size is None:
                raise ValueError(f"Cannot determine file size for {object_name}")

            _LOGGER.info(f"[download_file] Downloading {file_size // (1024*1024)}MB from GCS")

            # ✅ 스트리밍 다운로드 (대용량 파일 지원)
            # blob.open()은 스트림을 직접 반환하므로 메모리 효율적
            def stream_download():
                """스트리밍으로 파일 다운로드"""
                try:
                    with blob.open("rb", timeout=600) as f:  # 10분 타임아웃
                        while True:
                            chunk = f.read(1024 * 1024)  # 1MB씩 읽기
                            if not chunk:
                                break
                            yield chunk
                except Exception as e:
                    _LOGGER.error(f"[download_file] Stream error: {e}")
                    raise

            # AWS S3 스타일 응답 형식으로 반환
            return {
                'Body': stream_download(),
                'ContentLength': file_size
            }
        except Exception as e:
            _LOGGER.error(f'[download_file] Error: {e}')
            raise e

    @staticmethod
    def _generate_object_name(resource_group: str, file_id: str):
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
        else:
            return f"/files/unknown/{file_id}"
