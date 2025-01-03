from curses import meta
import logging
from os import name

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
            "file_id": utils.generate_id('file'),
            "name": file.filename,
            "resource_group": "SYSTEM",
        }

        file_svc = FileService(metadata)
        response: dict = file_svc.add(params)
        
        download_url = self.get_download_url(response)
        
        try:
            file_conn_mgr = FileConnectorManager()
            await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        except Exception as e:
            file_svc.delete(params)
            raise ERROR_FILE_UPLOAD_FAILED(name=response["name"])
        
        response["download_url"] = download_url
        resp = file_svc.update(response)
        return resp

    @router.get("/public/{file_id}")
    @exception_handler
    async def download_public_file(self, request: Request, file_id: str):

        # params, metdata = self.parse_request(request, self.token.credentials, "file", "get" )
        # file_svc = FileService(metdata)
        
        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": file_id,
            "resource_group": "SYSTEM",
        }

        file_svc = FileService(metadata)
        file_vo: dict = file_svc.get(params)

        download_url = file_vo["download_url"]
        if not download_url:
            raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id, name=file_vo["name"])

        try:
            file_conn_mgr = FileConnectorManager()
            file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
            if not file_stream:
                raise ERROR_FILE_DOWNLOAD_FAILED(name=file_vo["name"])
            
        except Exception as e:
            raise ERROR_FILE_DOWNLOAD_FAILED(name=file_vo["name"])

        return StreamingResponse(
            content=file_stream,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_vo['name']}"}
        )

    @router.post("/domain/upload")
    @exception_handler
    async def upload_domain_file(self, request: Request, file: UploadFile = File(...)):

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": utils.generate_id('file'),
            "name": file.filename,
            "resource_group": "DOMAIN",
        }

        file_svc = FileService(metadata)
        response: dict = file_svc.add(params)
        
        download_url = self.get_download_url(response)

        try:
            file_conn_mgr = FileConnectorManager()
            await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        except Exception as e:
            file_svc.delete(params)
            raise ERROR_FILE_UPLOAD_FAILED(name=response["name"])

        response["download_url"] = download_url
        resp = file_svc.update(response)
        return resp

    @router.get("/domain/{file_id}")
    @exception_handler
    async def download_domain_file(self, file_id:str, request: Request) -> StreamingResponse:

        # params, metdata = self.parse_request(request, self.token.credentials, "file", "get" )
        
        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": file_id,
            "resource_group": "DOMAIN",
        }
        
        file_svc = FileService(metadata)
        file_vo: dict = file_svc.get(params)
        
        download_url = file_vo["download_url"]
        if not download_url:
            raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id, name=file_vo["name"]) 
        
        try:
            file_conn_mgr = FileConnectorManager()
            file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
            if not file_stream:
                raise ERROR_FILE_DOWNLOAD_FAILED(name=file_vo["name"])
        
        except Exception as e:
            raise ERROR_FILE_DOWNLOAD_FAILED(name=file_vo["name"])
        

        return StreamingResponse(
            content=file_stream,
            media_type="binary/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_vo['name']}"}
        )

    @router.post("/workspace/upload")
    @exception_handler
    async def upload_workspace_file(self, request: Request, file: UploadFile = File(...)):
        
        # params, metdata = self.parse_request(request, self.token.credentials, "file", "add" )
        # file_svc = FileService(metdata)
        
        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": utils.generate_id('file'),
            "name": file.filename,
            "resource_group": "WORKSPACE",
        }
        file_svc = FileService(metadata)
        response: dict = file_svc.add(params)

        download_url = self.get_download_url(response)

        try:
            file_conn_mgr = FileConnectorManager()
            await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        except Exception as e:
            file_svc.delete(params)
            raise ERROR_FILE_UPLOAD_FAILED(name=response["name"])
        
        response["download_url"] = download_url
        resp = file_svc.update(response)
        return resp

    @router.get("/workspace/{file_id}")
    @exception_handler
    async def download_workspace_file(self, file_id:str, request: Request):

        # params, metdata = self.parse_request(request, self.token.credentials, "file", "get" )
        # file_svc = FileService(metdata)

        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": file_id,
            "resource_group": "WORKSPACE",
        }

        file_svc = FileService(metadata)
        file_vo: dict = file_svc.get(params)

        download_url = file_vo["download_url"]
        if not download_url:
            raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id, name=file_vo["name"])
        
        try:
            file_conn_mgr = FileConnectorManager()
            file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
            if not file_stream:
                raise ERROR_FILE_DOWNLOAD_FAILED(name=file_vo["name"])
        except Exception as e:
            raise ERROR_FILE_DOWNLOAD_FAILED(name=file_vo["name"])    


        return StreamingResponse(
            content=file_stream,
            media_type="binary/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_vo['name']}"}
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
            project_id = response["project_id"]
            download_url = "/files/domain/" + domain_id + "/project/"+ project_id + "/" +  file_id
        else:
            raise ERROR_NOT_SUPPORTED_RESOURCE_GROUP(resource_group=resource_group)
        
        return download_url