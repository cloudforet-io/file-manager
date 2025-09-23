import logging
import json
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
        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")
            
        try:
            object_name = self._generate_object_name(resource_group, file_id)
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)
            
            # 파일 존재 여부 확인
            return blob.exists()
        except Exception as e:
            _LOGGER.debug(f"[check_file] check_file error: {e}")
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
        
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)
            
            # BytesIO를 사용하여 메모리에서 업로드
            file_obj = BytesIO(data)
            blob.upload_from_file(file_obj)
        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
            raise e
        finally:
            file_obj.close()

    def download_file(self, resource_group: str, file_id: str):
        if self.client is None:
            raise Exception("GCPGCSConnector not initialized properly")

        object_name = self._generate_object_name(resource_group, file_id)

        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)
            # 파일 메타데이터 새로고침 (크기 정보 가져오기)
            blob.reload()
            # 파일 다운로드
            file_data = blob.download_as_bytes()
            # AWS S3 스타일 응답 형식으로 반환
            return {
                'Body': BytesIO(file_data),
                'ContentLength': blob.size
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