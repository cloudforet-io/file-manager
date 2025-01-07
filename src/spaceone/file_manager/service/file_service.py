import logging
from typing import Union

from spaceone.core import utils, config
from spaceone.core.service import *
from spaceone.file_manager.error.custom import *
from spaceone.file_manager.model.file.request import *
from spaceone.file_manager.model.file.response import *
from spaceone.file_manager.manager.file_manager import FileManager
from spaceone.file_manager.manager.file_connector_manager import FileConnectorManager
from spaceone.file_manager.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class FileService(BaseService):
    resource = "File"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_mgr = FileManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="file-manager:File.write",
        role_types=[
            "SYSTEM_ADMIN",
            "DOMAIN_ADMIN",
            "WORKSPACE_OWNER",
            "WORKSPACE_MEMBER",
        ],
    )
    @convert_model
    def add(self, params: FileAddRequest) -> Union[FileResponse, dict]:
        """Add file

        Args:
            params (FileAddRequest): {
                'name': 'str',              # required
                'reference': 'dict',
                'tags': 'dict',
                'resource_group': 'str',    # required
                'domain_id': 'str'          # injected from auth
                'workspace_id': 'str',      # injected from auth
                'project_id': 'str'         # injected from auth
            }

        Returns:
            FileResponse:
        """

        role_type = self.transaction.get_meta("authorization.role_type")
        
        if role_type == "SYSTEM_ADMIN":
            resource_group = "SYSTEM"
        elif role_type == "DOMAIN_ADMIN":
            resource_group = "DOMAIN"
        elif role_type == "WORKSPACE_OWNER" or role_type == "WORKSPACE_MEMBER":
            resource_group = "WORKSPACE"
        else:
            raise ERROR_PERMISSION_DENIED()
        
        if resource_group == "SYSTEM":
            params.domain_id = "*"
            params.workspace_id = "*"
            params.project_id = "*"
        elif resource_group == "DOMAIN":
            params.workspace_id = "*"
            params.project_id = "*"
        elif resource_group == "WORKSPACE" :
            self.identity_mgr.check_workspace(params.workspace_id, params.domain_id)
            params.project_id = "*"
        elif resource_group == "PROJECT":
            if not params.project_id:
                params.project_id = "*"
            else :
                self.identity_mgr.get_project(params.project_id, params.domain_id)


        file_vo = self.file_mgr.create_file(params.dict())
        return FileResponse(**file_vo.to_dict())

    @transaction(
        permission="file-manager:File.write",
        role_types=[
            "SYSTEM_ADMIN",
            "DOMAIN_ADMIN",
            "WORKSPACE_OWNER",
            "WORKSPACE_MEMBER",
        ],
    )
    @convert_model
    def update(self, params: FileUpdateRequest) -> Union[FileResponse, dict]:
        """Update file

        Args:
            params (FileUpdateRequest): {
                'file_id': 'str',           # required
                'reference': 'dict',
                'tags': 'dict',
                'download_url': 'str',
                'domain_id': 'str'          # injected from auth
                'workspace_id': 'str',      # injected from auth
                'project_id': 'str'         # injected from auth
            }

        Returns:
            FileResponse:
        """

        file_vo = self.file_mgr.get_file(
            params.file_id,
            params.domain_id,
            params.workspace_id,
            params.project_id
        )

        file_vo = self.file_mgr.update_file_by_vo(
            params.dict(exclude_unset=True), file_vo
        )

        return FileResponse(**file_vo.to_dict())

    @transaction(
        permission="file-manager:File.write",
        role_types=[
            "SYSTEM_ADMIN",
            "DOMAIN_ADMIN",
            "WORKSPACE_OWNER",
            "WORKSPACE_MEMBER",
        ],
    )
    @convert_model
    def delete(self, params: FileDeleteRequest) -> None:
        """Delete file

        Args:
            params (FileDeleteRequest): {
                'file_id': 'str',           # required
                'domain_id': 'str'          # injected from auth
                'workspace_id': 'str',      # injected from auth
                'project_id': 'str'         # injected from auth
            }

        Returns:
            None:
        """

        file_vo = self.file_mgr.get_file(
            params.file_id,
            params.domain_id,
            params.workspace_id,
            params.project_id,
        )
        
        resource_group = file_vo["resource_group"]
        file_id = file_vo["file_id"]
        
        try:
            file_conn_mgr = FileConnectorManager()
            file_conn_mgr.delete_file(resource_group, file_id)
        except Exception as e:
            logging.error(f'[ERROR] Failed to delete file : {file_vo.name} ({file_vo.file_id})')
            raise ERROR_FILE_DELETE_FAILED(file_id=file_id)

        self.file_mgr.delete_file_by_vo(file_vo)

    @transaction(
        permission="file-manager:File.read",
        role_types=[
            "SYSTEM_ADMIN",
            "DOMAIN_ADMIN",
            "WORKSPACE_OWNER",
            "WORKSPACE_MEMBER",
        ],
    )

    @change_value_by_rule("APPEND", "domain_id", "*")
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "project_id", "*")
    @convert_model
    def get(self, params: FileGetRequest) -> Union[FileResponse, dict]:
        """Get file

        Args:
            params (FileGetRequest): {
                'file_id': 'str',           # required
                'resource_group': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth
                'project_id': 'str'         # injected from auth
                'user_projects': 'list'     # injected from auth
            }

        Returns:
            FileResponse:
        """

        file_vo = self.file_mgr.get_file(
            params.file_id,
            params.domain_id,
            params.workspace_id,
            params.project_id,
        )

        return FileResponse(**file_vo.to_dict())

    @transaction(
        permission="file-manager:File.read",
        role_types=[
            "SYSTEM_ADMIN",
            "DOMAIN_ADMIN",
            "WORKSPACE_OWNER",
            "WORKSPACE_MEMBER",
        ],
    )
    @change_value_by_rule("APPEND", "domain_id", "*")
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(
        [
            "file_id",
            "name",
            "resource_type",
            "resource_id",
            "domain_id",
            "workspace_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["file_id", "name"])
    @convert_model
    def list(self, params: FileSearchQueryRequest) -> Union[FilesResponse, dict]:
        """List files

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'file_id': 'str',
                'name': 'str',
                'resource_type': 'str',
                'resource_id': 'str',
                'domain_id': 'str',                             # injected from auth
                'workspace_id': 'str',                          # injected from auth
                'user_projects': 'list',                        # injected from auth
            }

        Returns:
            FilesResponse:
        """

        query = params.query or {}
        file_vos, total_count = self.file_mgr.list_files(query)
        files_info = [file_vo.to_dict() for file_vo in file_vos]

        return FilesResponse(results=files_info, total_count=total_count)

    @transaction(
        permission="file-manager:File.read",
        role_types=[
            "SYSTEM_ADMIN",
            "DOMAIN_ADMIN",
            "WORKSPACE_OWNER",
            "WORKSPACE_MEMBER",
        ],
    )
    @change_value_by_rule("APPEND", "domain_id", "*")
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(["domain_id", "workspace_id", "user_projects"])
    @append_keyword_filter(["file_id", "name"])
    @convert_model
    def stat(self, params: FileStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'domain_id': 'str',                                     # injected from auth
                'workspace_id': 'str',                                  # injected from auth
            }

        Returns:
            dict:
        """

        query = params.query or {}
        return self.file_mgr.stat_files(query)
