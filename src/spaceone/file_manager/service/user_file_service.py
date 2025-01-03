import logging
from typing import Union

from spaceone.core import utils
from spaceone.core.service import *
from spaceone.file_manager.error.custom import *
from spaceone.file_manager.model.user_file.request import *
from spaceone.file_manager.model.user_file.response import *
from spaceone.file_manager.manager.user_file_manager import UserFileManager
from spaceone.file_manager.manager.file_connector_manager import FileConnectorManager
from spaceone.file_manager.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class UserFileService(BaseService):
    resource = "UserFile"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_file_mgr = UserFileManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="file-manager:UserFile.write",
        role_types=["USER"],
    )
    @convert_model
    def add(self, params: UserFileAddRequest) -> Union[UserFileResponse, dict]:
        """Add file

        Args:
            params (UserFileAddRequest): {
                'name': 'str',              # required
                'reference': 'dict',
                'tags': 'dict',
                'domain_id': 'str',         # injected from auth
                'user_id': 'str',           # injected from auth
            }
        Returns:
            UserFileResponse:
        """

        user_file_vo = self.user_file_mgr.create_file(params.dict())
        return UserFileResponse(**user_file_vo.to_dict())

    @transaction(
        permission="file-manager:UserFile.write",
        role_types=[
            "USER",
        ],
    )
    @convert_model
    def update(self, params: UserFileUpdateRequest) -> Union[UserFileResponse, dict]:
        """Update file

        Args:
            params (FileUpdateRequest): {
                'file_id': 'str',           # required
                'reference': 'dict',
                'tags': 'dict',
                'download_url': 'str',
                'user_id': 'str'            # injected from auth
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            UserFileResponse:
        """

        file_vo = self.user_file_mgr.get_file(
            params.file_id,
            params.domain_id, 
            params.user_id
        )
        
        file_vo = self.user_file_mgr.update_file_by_vo(params.dict(exclude_unset=True), file_vo)

        return UserFileResponse(**file_vo.to_dict())

    @transaction(
        permission="file-manager:UserFile.write",
        role_types=["USER"],
    )
    @convert_model
    def delete(self, params: UserFileDeleteRequest) -> None:
        """Delete file

        Args:
            params (FileDeleteRequest): {
                'file_id': 'str',           # required
                'user_id': 'str'            # injected from auth
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            None:
        """

        user_file_vo = self.user_file_mgr.get_file(
            params.file_id,
            params.domain_id,
            params.user_id,
        )
        
        try:
            file_conn_mgr = FileConnectorManager()
            file_conn_mgr.delete_file("user_file", user_file_vo.file_id)
        except Exception as e:
            _LOGGER.error(f"[delete] Failed to delete file: {user_file_vo.file_id}")
            raise ERROR_FILE_DELETE_FAILED(name=download_url)
        
        self.userfile_mgr.delete_file_by_vo(file_vo)

    @transaction(
        permission="file-manager:UserFile.read",
        role_types=["USER"],
    )
    @convert_model
    def get(self, params: UserFileGetRequest) -> Union[UserFileResponse, dict]:
        """Get file

        Args:
            params (FileGetRequest): {
                'file_id': 'str',           # required
                'user_id': 'str',           # injected from auth
                'domain_id': 'str'          # injected from auth
            }

        Returns:
            UserFileResponse:
        """

        file_vo = self.userfile_mgr.get_file(
            params.file_id, 
            params.domain_id, 
            params.user_id,
        )

        return UserFileResponse(**file_vo.to_dict())

    @transaction(
        permission="file-manager:UserFile.read",
        role_types=["USER"],
    )
    @append_query_filter(
        [
            "file_id",
            "name",
            "resource_type",
            "resource_id",
            "domain_id",
            "user_id",
        ]
    )
    @append_keyword_filter(["file_id", "name"])
    @convert_model
    def list(self, params: UserFileSearchQueryRequest) -> Union[UserFilesResponse, dict]:
        """List files

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'file_id': 'str',
                'name': 'str',
                'resource_type': 'str',
                'resource_id': 'str',
                'domain_id': 'str',                             # injected from auth
                'user_id': 'str',                               # injected from auth
            }

        Returns:
            UserFilesResponse:
        """

        query = params.query or {}
        file_vos, total_count = self.userfile_mgr.list_files(query)
        files_info = [file_vo.to_dict() for file_vo in file_vos]

        return UserFilesResponse(results=files_info, total_count=total_count)

    @transaction(
        permission="file-manager:UserFile.read",
        role_types=["USER"],
    )
    @append_query_filter(["domain_id", "user_id"])
    @append_keyword_filter(["file_id", "name"])
    @convert_model
    def stat(self, params: UserFileStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'domain_id': 'str',                                     # injected from auth
                'user_id': 'str',                                  # injected from auth
            }

        Returns:
            dict:
        """

        query = params.query or {}
        return self.userfile_mgr.stat_files(query)

