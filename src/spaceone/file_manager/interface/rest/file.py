import logging
from urllib.parse import quote
from typing import Optional
from fastapi import Request, Depends, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse
from fastapi_utils.cbv import cbv
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi_utils.inferring_router import InferringRouter


from spaceone.core import utils
from spaceone.core.fastapi.api import BaseAPI, exception_handler
from spaceone.file_manager.manager.streaming_file_connector_manager import StreamingFileConnectorManager
from spaceone.file_manager.service.file_service import FileService
from spaceone.file_manager.error import *

_LOGGER = logging.getLogger(__name__)
_AUTH_SCHEME = HTTPBearer(auto_error=False)

router = InferringRouter(include_in_schema=False)


@cbv(router)
class Files(BaseAPI):
    token: HTTPAuthorizationCredentials = Depends(_AUTH_SCHEME)
    service = "file-manager"

    @router.post("/public/upload")
    @exception_handler
    async def upload_public_file(self, request: Request, file: UploadFile = File(...)):

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "name": file.filename,
            "resource_group": "SYSTEM",
        }

        file_info = self.upload_file(metadata, params, file)
        return file_info

    @router.get("/public/{file_id}")
    @exception_handler
    async def download_public_file(self, request: Request, file_id: str, token:str):

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
        }

        return self.download_file(metadata, params)

    @router.post("/domain/upload")
    @exception_handler
    async def upload_domain_file(self, request: Request, file: UploadFile = File(...)):

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "name": file.filename,
            "resource_group": "DOMAIN",
        }

        file_info = self.upload_file(metadata, params, file)
        return file_info

    @router.get("/domain/{file_id}")
    @exception_handler
    async def download_domain_file(self, file_id:str, token:str, request: Request) -> StreamingResponse:

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
        }

        return self.download_file(metadata, params)

    @router.post("/workspace/upload")
    @exception_handler
    async def upload_workspace_file(self, request: Request, file: UploadFile = File(...)):

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "name": file.filename,
            "resource_group": "WORKSPACE",
        }
        file_info = self.upload_file(metadata, params, file)
        return file_info

    @router.get("/workspace/{file_id}")
    @exception_handler
    async def download_workspace_file(self, file_id:str, token:str, request: Request):

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
        }

        return self.download_file(metadata, params)


    @router.post("/project/upload")
    @exception_handler
    async def upload_project_file(
        self,
        request: Request,
        file: UploadFile = File(...),
        project_id: Optional[str] = None,
    ):

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "name": file.filename,
            "resource_group": "PROJECT",
        }


        if project_id:
            params["project_id"] = project_id
        else:
            params["project_id"] = "*"

        file_info = self.upload_file(metadata, params, file)
        # file_info가 dict가 아닌 경우 변환
        if hasattr(file_info, 'to_dict'):
            return file_info.to_dict()
        elif isinstance(file_info, dict):
            return file_info
        else:
            return {"file_id": str(file_info)}

    @router.get("/project/{file_id}")
    @exception_handler
    async def download_project_file(self, file_id:str, token:str, request: Request)-> StreamingResponse:

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
        }

        return self.download_file(metadata, params)

    def upload_file(self, metadata, params, file) :

        try:
            file_svc = FileService(metadata)
            file_info: dict = file_svc.add(params)

            resource_group = file_info["resource_group"]
            file_id = file_info["file_id"]

            # 동기 스트리밍 커넥터 사용
            file_conn_mgr = StreamingFileConnectorManager()

            # 스트리밍 업로드 사용 - 청크 단위로 파일 처리 (메모리 효율적)
            _LOGGER.info(f"[upload_file] Starting streaming upload for file_id: {file_id}")

            # 동기 방식으로 스트리밍 업로드 실행
            file_conn_mgr.stream_upload_file(resource_group, file_id, file)
            _LOGGER.info(f"[upload_file] Streaming upload completed for file_id: {file_id}")

        except Exception as e:
            _LOGGER.error(f'[upload_file] Error: {e}')
            if 'file_id' in locals() and 'file_svc' in locals():
                file_svc.delete({"file_id":file_id})
            raise ERROR_FILE_UPLOAD_FAILED(name=file_info["name"] if 'file_info' in locals() else "unknown")

        return file_info

    def download_file(self, metadata, params) -> StreamingResponse:

        file_svc = FileService(metadata)
        file_info: dict = file_svc.get(params)

        resource_group = file_info["resource_group"]
        file_id = file_info["file_id"]

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
                    raise ERROR_FILE_DOWNLOAD_FAILED(name=file_info["name"])

        except Exception as e:
            _LOGGER.error(f'[download_file] Error: {e}')
            raise ERROR_FILE_DOWNLOAD_FAILED(name=file_info["name"])

        filename = quote(file_info['name'])

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
