from curses import meta
import logging
from turtle import down

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
        
        params = {
            "token": self.token.credentials,
            "name": file.filename,
            "resource_group": "SYSTEM",
        }
        file_svc = FileService()
        response: dict = file_svc.add(params)
        
        download_url = self.get_download_url(response)
        
        # Update File
        file_conn_mgr = FileConnectorManager()
        # file_conn_mgr.upload_file(download_url, await file.read())
        await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        
        response["download_url"] = download_url
        file_svc.update(response)
        return response

    @router.get("/public/{file_id}")
    @exception_handler
    async def download_public_file(self, request: Request, file_id: str):

        params = {
            "token": self.token.credentials,
            "file_id": file_id,
            "resource_group":"SYSTEM",
        }

        file_svc =FileService()
        file_vo: dict = file_svc.get(params)

        download_url = file_vo["download_url"]
        if not download_url:
            raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id)

        file_conn_mgr = FileConnectorManager()
        file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
        if not file_stream:
                raise ERROR_FILE_DOWNLOAD_FAILED(file_id=file_vo["file_id"])

        return StreamingResponse(
            content=file_stream,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_vo["name"]}"}
        )

    @router.post("/domain/{domain_id}/upload")
    @exception_handler
    async def upload_domain_file(self, domain_id, request: Request, file: UploadFile = File(...)):
        

        params = {
            "token": self.token.credentials,
            "name": file.filename,
            "domain_id": domain_id,
            "resource_group": "DOMAIN",
        }
        file_svc = FileService()
        response: dict = file_svc.add(params)
        
        download_url = self.get_download_url(response)

        # Update File
        file_conn_mgr = FileConnectorManager()
        await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())

        response["download_url"] = download_url
        response = file_svc.update(response)
        return response

    @router.get("/domain/{domain_id}/{file_id}")
    @exception_handler
    async def download_domain_file(self, domain_id:str, file_id:str, request: Request) -> StreamingResponse:

        try:
            params = {
                "token": self.token.credentials,
                "file_id": file_id,
                "domain_id": domain_id,
                "resource_group":"DOMAIN",
            }

            file_svc = FileService()
            file_vo: dict = file_svc.get(params)
            
            download_url = file_vo["download_url"]
            if not download_url:
                raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id)
            
            file_conn_mgr = FileConnectorManager()
            file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
            if not file_stream:
                raise ERROR_FILE_DOWNLOAD_FAILED(file_id=file_vo["file_id"])

            return StreamingResponse(
                content=file_stream,
                media_type="binary/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={file_vo["name"]}"}
            )
        except Exception as e:
            raise ERROR_FILE_DOWNLOAD_FAILED(file_id=file_id)

    @router.post("/domain/{domain_id}/workspace/{workspace_id}/upload")
    @exception_handler
    async def upload_workspace_file(self, domain_id:str, workspace_id:str, request: Request, file: UploadFile = File(...)):
        
        params = {
            "token": self.token.credentials,
            "name": file.filename,
            "domain_id": domain_id,
            "workspace_id": workspace_id,
            "resource_group": "WORKSPACE",
        }
        file_svc = FileService()
        response: dict = file_svc.add(params)

        download_url = self.get_download_url(response)

        # Update File
        file_conn_mgr = FileConnectorManager()
        await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        
        response["download_url"] = download_url
        file_svc.update(response)
        return response

    @router.get("/domain/{domain_id}/workspace/{workspace_id}/{file_id}")
    @exception_handler
    async def download_workspace_file(self, domain_id:str, workspace_id:str, file_id:str, request: Request):

        params = {
            "token": self.token.credentials,
            "file_id": file_id,
            "domain_id": domain_id,
            "workspace_id": workspace_id,
            "resource_group": "WORKSPACE",
        }

        file_svc =FileService()
        file_vo: dict = file_svc.get(params)

        download_url = file_vo["download_url"]
        if not download_url:
            raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id)
        
        
        file_conn_mgr = FileConnectorManager()
        file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
        if not file_stream:
            raise ERROR_FILE_DOWNLOAD_FAILED(file_id=file_vo["file_id"])

        return StreamingResponse(
            content=file_stream,
            media_type="binary/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_vo["name"]}"}
        )

    def get_download_url(self, response: dict ) -> str:
        
        resource_group = response["resource_group"]
        file_id = response["file_id"]
        
        if resource_group == "SYSTEM":
            download_url = "/files/public/" + file_id
        elif resource_group == "DOMAIN":
            domain_id = response["domain_id"]
            download_url = "/files/domain/" + domain_id + "/" + file_id
        elif resource_group == "WORKSPACE":
            domain_id = response["domain_id"]
            workspace_id = response["workspace_id"]
            download_url = "/files/domain/" + domain_id + "/workspace/" + workspace_id + "/" + file_id
        elif resource_group == "PROJECT":
            domain_id = response["domain_id"]
            user_id = response["user_id"]
            download_url = "/files/domain/" + domain_id + "/user/"+ user_id + "/" +  file_id
        else:
            raise ERROR_NOT_SUPPORTED_RESOURCE_GROUP(resource_group=resource_group)
        
        return download_url