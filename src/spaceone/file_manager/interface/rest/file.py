import logging


from urllib.parse import quote
from typing import Optional
from fastapi import Request, Depends, File, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.concurrency import run_in_threadpool
from fastapi_utils.cbv import cbv
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi_utils.inferring_router import InferringRouter


from spaceone.core import utils
from spaceone.core.fastapi.api import BaseAPI, exception_handler
from spaceone.file_manager.manager.file_connector_manager import FileConnectorManager
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

        file_info = await self.upload_file(metadata, params, file)
        return file_info

    @router.get("/public/{file_id}")
    @exception_handler
    async def download_public_file(self, request: Request, file_id: str, token:str):

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
            "resource_group": "SYSTEM",
        }

        return await self.download_file(metadata, params)

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

        file_info = await self.upload_file(metadata, params, file)
        return file_info

    @router.get("/domain/{file_id}")
    @exception_handler
    async def download_domain_file(self, file_id:str, token:str, request: Request) -> StreamingResponse:

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
            "resource_group": "DOMAIN",
        }
        
        return await self.download_file(metadata, params)

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
        file_info = await self.upload_file(metadata, params, file)
        return file_info

    @router.get("/workspace/{file_id}")
    @exception_handler
    async def download_workspace_file(self, file_id:str, token:str, request: Request):

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
            "resource_group": "WORKSPACE",
        }

        return await self.download_file(metadata, params)


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
        
        file_info = await self.upload_file(metadata, params, file)
        return file_info

    @router.get("/project/{file_id}")
    @exception_handler
    async def download_project_file(self, file_id:str, token:str, request: Request)-> StreamingResponse:

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
            "resource_group": "PROJECT",
        }

        return await self.download_file(metadata, params)

    async def upload_file(self, metadata, params, file) :
        
        try:

            file_svc = FileService(metadata)
            file_info: dict = file_svc.add(params)

            resource_group = file_info["resource_group"]
            file_id = file_info["file_id"]

            file_conn_mgr = FileConnectorManager()
            await run_in_threadpool(file_conn_mgr.upload_file, resource_group, file_id, await file.read())
        except Exception as e:
            file_svc.delete({"file_id":file_id})
            raise ERROR_FILE_UPLOAD_FAILED(name=file_info["name"])

        return file_info

    async def download_file(self, metadata, params) -> StreamingResponse:

        file_svc = FileService(metadata)
        file_info: dict = file_svc.get(params)
        
        resource_group = file_info["resource_group"]
        file_id = file_info["file_id"]

        try:
            file_conn_mgr = FileConnectorManager()
            obj = await run_in_threadpool(file_conn_mgr.download_file, resource_group, file_id)
            if not obj:
                raise ERROR_FILE_DOWNLOAD_FAILED(name=file_info["name"])
            
        except Exception as e:
            raise ERROR_FILE_DOWNLOAD_FAILED(name=file_info["name"])


        filename = quote(file_info['name'])
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
            "content-length": str(obj['ContentLength']),
        }

        return StreamingResponse(
            content=obj["Body"],
            media_type="application/octet-stream",
            headers=headers,
        )
