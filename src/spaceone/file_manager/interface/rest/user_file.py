
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
        
        metadata = {
            "token": self.token.credentials,
        }
        params = {
            "name": file.filename,
            "resource_group": "USER",
        }
        user_file_svc = UserFileService(metadata)
        user_file_info: dict = user_file_svc.add(params)
        
        resource_group = "USER"
        file_id = user_file_info["file_id"]
        
        try:
            file_conn_mgr = FileConnectorManager()
            await run_in_threadpool(file_conn_mgr.upload_file, resource_group,file_id, await file.read())
        except Exception as e:
            user_file_svc.delete({"file_id":file_id})
            raise ERROR_FILE_UPLOAD_FAILED(name=user_file_info["name"])
        
        return user_file_info

    @router.get("/user/{file_id}")
    @exception_handler
    async def download_user_file(self, file_id:str, token:str,  request: Request):

        metadata = {
            "token": token,
        }
        params = {
            "file_id": file_id,
            "resource_group": "USER",
        }
        user_file_svc = UserFileService(metadata)
        user_file_info: dict = user_file_svc.get(params)
        
        resource_group = "USER"

        try:
            file_conn_mgr = FileConnectorManager()
            file_stream = await run_in_threadpool(file_conn_mgr.download_file, resource_group, file_id)
            if not file_stream:
                raise ERROR_FILE_DOWNLOAD_FAILED(name=user_file_info["name"])
        
        except Exception as e:
            raise ERROR_FILE_DOWNLOAD_FAILED(name=user_file_info["name"])

        return StreamingResponse(
            content=file_stream,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={user_file_info['name']}"}
        )