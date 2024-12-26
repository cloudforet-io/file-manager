import logging

from fastapi import Request, Depends, File, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi_utils.cbv import cbv
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi_utils.inferring_router import InferringRouter
from spaceone.core.fastapi.api import BaseAPI, exception_handler
from spaceone.file_manager.manager.file_connector_manager import FileConnectorManager

from spaceone.file_manager.service.file_service import FileService

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

        file_svc = FileService(metadata)
        response: dict = await run_in_threadpool(file_svc.add, params)

        file_id = response.get("file_id")

        file_conn_mgr = FileConnectorManager()
        # Update File
        # file_id = file_vo.file_id

        return response

    @router.get("/public/{file_id}")
    @exception_handler
    async def download_public_file(self, request: Request, file_id: str):
        metadata = {
            "token": self.token.credentials,
        }

        params = {
            "file_id": file_id,
        }

        file_svc = FileService(metadata)
        response: dict = await run_in_threadpool(file_svc.get, params)

        file_id = response.get("file_id")

        file_conn_mgr = FileConnectorManager()
        # Update File
        # file_id = file_vo.file_id

        return response
