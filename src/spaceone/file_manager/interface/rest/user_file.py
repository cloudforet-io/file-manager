
from curses import meta
import logging

from fastapi import Request, Depends, File, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.concurrency import run_in_threadpool
from fastapi_utils.cbv import cbv
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi_utils.inferring_router import InferringRouter


from spaceone.core import utils
from spaceone.core.fastapi.api import BaseAPI, exception_handler
from spaceone.file_manager.manager.file_connector_manager import FileConnectorManager
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
        
        # params, metdata = self.parse_request(request, self.token.credentials, "userfile", "add" )

        # userfile_svc = UserFileService(metdata)
        
        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": utils.generate_id('file'),
            "name": file.filename,
            "resource_group": "USER",
        }
        userfile_svc = UserFileService(metadata)
        response: dict = userfile_svc.add(params)
        
        download_url = self.get_download_url(response)
        
        try:
            file_conn_mgr = FileConnectorManager()
            await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        except Exception as e:
            userfile_svc.delete(params)
            raise ERROR_FILE_UPLOAD_FAILED(name=response["name"])
        
        response["download_url"] = download_url
        resp = userfile_svc.update(response)
        return resp

    @router.get("/user/{file_id}")
    @exception_handler
    async def download_user_file(self, file_id:str, request: Request):

        # params, metdata = self.parse_request(request, self.token.credentials, "userfile", "add" )
        # userfile_svc = UserFileService(metdata)
        
        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "file_id": file_id,
            "resource_group": "USER",
        }
        userfile_svc = UserFileService(metadata)
        file_vo: dict = userfile_svc.get(params)

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
        
        file_id = response["file_id"]
        domain_id = response["domain_id"]
        user_id = response["user_id"]
        
        download_url = "/files/domain/" + domain_id + "/user/"+ user_id + "/" +  file_id
        
        return download_url