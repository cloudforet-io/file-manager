
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

# @cbv(router)
# class Files(BaseAPI):
#     token: HTTPAuthorizationCredentials = Depends(_AUTH_SCHEME)
#     service = "file-manager"
    
#     @router.post("/domain/{domain_id}/user/{user_id}/upload")
#     @exception_handler
#     async def upload_user_file(self, domain_id:str, user_id:str, request: Request, file: UploadFile = File(...)):
        
#         params = {
#             "token": self.token.credentials,
#             "name": file.filename,
#             "domain_id": domain_id,
#             "user_id": user_id,
#             "resource_group": "PROJECT",
#         }
#         file_svc = FileService()
#         response: dict = file_svc.add(params)
        
#         download_url = self.get_download_url(response)
        
#         # Update File
#         file_conn_mgr = FileConnectorManager()
#         await run_in_threadpool(file_conn_mgr.upload_file, download_url, await file.read())
        
#         response["download_url"] = download_url
#         file_svc.update(response)
#         return response

#     @router.get("/domain/{domain_id}/user/{user_id}/{file_id}")
#     @exception_handler
#     async def download_user_file(self, domain_id:str, user_id:str, file_id:str, request: Request):

#         params = {
#             "token": self.token.credentials,
#             "file_id": file_id,
#             "domain_id": domain_id,
#             "user_id": user_id,
#             "resource_group": "PROJECT",
#         }

#         file_svc =FileService()
#         file_vo: dict = file_svc.get(params)

#         download_url = file_vo["download_url"]
#         if not download_url:
#             raise ERROR_FILE_DOWNLOAD_URL_EXIST(file_id=file_id)
        
#         file_conn_mgr = FileConnectorManager()
#         file_stream = await run_in_threadpool(file_conn_mgr.download_file, download_url)
#         if not file_stream:
#             raise ERROR_FILE_DOWNLOAD_FAILED(file_id=file_vo["file_id"])

#         return StreamingResponse(
#             content=file_stream,
#             media_type="binary/octet-stream",
#             headers={"Content-Disposition": f"attachment; filename={file_vo["name"]}"}
#         )