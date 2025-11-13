
import logging
from urllib.parse import quote
from fastapi import Request, Depends, File, UploadFile
from fastapi.responses import StreamingResponse
from fastapi_utils.cbv import cbv
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi_utils.inferring_router import InferringRouter


from spaceone.core import utils
from spaceone.core.fastapi.api import BaseAPI, exception_handler
from spaceone.file_manager.manager.streaming_file_connector_manager import StreamingFileConnectorManager
from spaceone.file_manager.model import user_file
from spaceone.file_manager.service.user_file_service import UserFileService
from spaceone.file_manager.error import *

_LOGGER = logging.getLogger(__name__)
_AUTH_SCHEME = HTTPBearer(auto_error=False)

router = InferringRouter(include_in_schema=False)

@cbv(router)
class UserFiles(BaseAPI):
    token: HTTPAuthorizationCredentials = Depends(_AUTH_SCHEME)
    service = "file-manager"

    @router.post("/user/upload")
    @exception_handler
    async def upload_user_file(self, request: Request, file: UploadFile = File(...)):

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "name": file.filename,
        }

        user_file_info = self.upload_file(metadata, params, file)
        return user_file_info

    @router.get("/user/{file_id}")
    @exception_handler
    async def download_user_file(self, file_id:str, token:str,  request: Request):

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
        }

        return self.download_file(metadata, params)

    def upload_file(self, metadata, params, file) :
        user_file_info = None
        file_id = None

        try:
            user_file_svc = UserFileService(metadata)
            user_file_info: dict = user_file_svc.add(params)

            resource_group = "USER"
            file_id = user_file_info["file_id"]

            # 동기 스트리밍 커넥터 사용
            file_conn_mgr = StreamingFileConnectorManager()

            # 스트리밍 업로드 사용 - 청크 단위로 파일 처리 (메모리 효율적)
            _LOGGER.info(f"[upload_file] Starting streaming upload for file_id: {file_id}")

            # 동기 방식으로 스트리밍 업로드 실행
            file_conn_mgr.stream_upload_file(resource_group, file_id, file)
            _LOGGER.info(f"[upload_file] Streaming upload completed for file_id: {file_id}")

        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
            # 업로드 실패 시 DB에서 파일 정보 삭제
            if user_file_info and file_id:
                try:
                    user_file_svc.delete({"file_id": file_id})
                except Exception as delete_error:
                    _LOGGER.error(f'[upload_file] Failed to cleanup file record: {delete_error}')

            # 파일명이 있으면 사용, 없으면 기본 메시지
            file_name = user_file_info.get("name", "unknown") if user_file_info else params.get("name", "unknown")
            raise ERROR_FILE_UPLOAD_FAILED(name=file_name)

        return user_file_info

    def download_file(self, metadata, params) -> StreamingResponse:

        user_file_svc = UserFileService(metadata)
        user_file_info: dict = user_file_svc.get(params)

        resource_group = "USER"
        file_id = user_file_info["file_id"]

        try:
            # 동기 스트리밍 커넥터 사용
            file_conn_mgr = StreamingFileConnectorManager()

            # 스트리밍 다운로드를 위한 동기 제너레이터
            def stream_generator():
                try:
                    for chunk in file_conn_mgr.download_file_stream(resource_group, file_id):
                        yield chunk
                except Exception as e:
                    _LOGGER.error(f"[download_file] Error during streaming: {e}")
                    raise ERROR_FILE_DOWNLOAD_FAILED(name=user_file_info["name"])

        except Exception as e:
            _LOGGER.error(f'[download_file] Error: {e}')
            raise ERROR_FILE_DOWNLOAD_FAILED(name=user_file_info["name"])

        filename = quote(user_file_info['name'])

        # 스트리밍 응답 헤더
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
            # Content-Length는 스트리밍이므로 설정하지 않음
            "Cache-Control": "no-cache",
        }

        # 동기 스트리밍 응답 반환
        return StreamingResponse(
            stream_generator(),
            media_type="application/octet-stream",
            headers=headers,
        )
