import logging
from typing import Tuple, Union

from spaceone.core.service import *
from spaceone.file_manager.error import *
from spaceone.file_manager.model.file_model import File
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
        self.file_mgr: FileManager = self.locator.get_manager(FileManager)
        self.identity_mgr: IdentityManager = self.locator.get_manager(IdentityManager)

    @transaction(
        permission="file-manager:File.write",
        role_types=["SYSTEM_ADMIN", "DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["name", "resource_group"])
    def add(self, params: dict) -> Tuple[File, str, dict]:
        """Add file

        Args:
            params (dict): {
                'name': 'str',              # required
                'tags': 'dict',
                'reference': 'dict',
                'resource_group',           # required
                'workspace_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            file_vo
        """
        resource_group = params["resource_group"]
        workspace_id = params.get("workspace_id")
        domain_id = params.get("domain_id")
        params["file_type"] = self._get_file_type(params["name"])

        if resource_group == "SYSTEM":
            params["domain_id"] = "*"
            params["workspace_id"] = "*"
        elif resource_group == "DOMAIN":
            params["workspace_id"] = "*"
        else:
            self.identity_mgr.check_workspace(workspace_id, domain_id)

        file_vo: File = self.file_mgr.create_file(params)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager(
            FileConnectorManager
        )
        upload_url, upload_options = file_conn_mgr.get_upload_url(
            file_vo.file_id, file_vo.name
        )

        return file_vo, upload_url, upload_options

    @transaction(
        permission="file-manager:File.write",
        role_types=["SYSTEM_ADMIN", "DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["file_id"])
    def update(self, params: dict) -> File:
        """Update file

        Args:
            params (dict): {
                'file_id': 'str',        # required
                'tags': 'dict',
                'reference': 'dict',
                'workspace_id': 'str',   # injected from auth
                'domain_id': 'str'       # injected from auth
            }

        Returns:
            file_vo
        """

        workspace_id = params.get("workspace_id")
        domain_id = params.get("domain_id")

        file_id = params["file_id"]
        file_vo: File = self.file_mgr.get_file(file_id, workspace_id, domain_id)
        file_vo = self.file_mgr.update_file_by_vo(params, file_vo)

        return file_vo

    @transaction(
        permission="file-manager:File.write",
        role_types=["SYSTEM_ADMIN", "DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["file_id"])
    def delete(self, params: dict) -> None:
        """Delete file

        Args:
            params (dict): {
                'file_id': 'str',        # required
                'workspace_id': 'str',   # injected from auth
                'domain_id': 'str'       # injected from auth
            }

        Returns:
            None
        """

        file_id = params["file_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params.get("domain_id")

        file_vo: File = self.file_mgr.get_file(file_id, workspace_id, domain_id)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager(
            FileConnectorManager
        )
        file_conn_mgr.delete_file(file_id, file_vo.name)

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
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["file_id"])
    def get_download_url(self, params: dict) -> Tuple[File, Union[str, None]]:
        """Get download url of file

        Args:
            params (dict): {
                'file_id': 'str',         # required
                'workspace_id': 'str',    # injected from auth
                'domain_id': 'str'        # injected from auth
                'user_projects': 'list'   # injected from auth
            }

        Returns:
            file_data (dict)
        """

        file_id = params["file_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params.get("domain_id")

        file_vo: File = self.file_mgr.get_file(file_id, workspace_id, domain_id)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager(
            FileConnectorManager
        )

        if file_vo.state == "PENDING":
            if not file_conn_mgr.check_file(file_id, file_vo.name):
                raise ERROR_FILE_UPLOAD_STATE()

            conditions = {"state": "DONE"}
            if workspace_id:
                conditions["workspace_id"] = workspace_id
            if domain_id:
                conditions["domain_id"] = domain_id

            file_vo = self.file_mgr.update_file_by_vo(conditions, file_vo)

        download_url = file_conn_mgr.get_download_url(
            file_id, file_vo.name, file_vo.domain_id
        )

        return file_vo, download_url

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
    @check_required(["file_id"])
    def get(self, params: dict) -> File:
        """Get file

        Args:
            params (dict): {
                'file_id': 'str',          # required
                'workspace_id': 'str',     # injected from auth
                'domain_id': 'str'         # injected from auth
                'user_projects': 'list'    # injected from auth
            }

        Returns:
            file_vo
        """

        file_id = params["file_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params.get("domain_id")
        user_projects = params.get("user_projects")

        return self.file_mgr.get_file(file_id, workspace_id, domain_id, user_projects)

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
            "state",
            "file_type",
            "resource_type",
            "resource_id",
            "resource_group",
            "workspace_id",
            "domain_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["file_id", "name"])
    def list(self, params: dict) -> dict:
        """List files

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'file_id': 'str',
                'name': 'str',
                'state': 'str',
                'file_type': 'str',
                'resource_type': 'str',
                'resource_id': 'str',
                'resource_group': 'str',
                'workspace_id': 'str',
                'domain_id': 'str'
                'user_projects': 'list'                             # injected from auth
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.file_mgr.list_files(query)

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
    @check_required(["query"])
    @append_query_filter(["user_projects", "workspace_id", "domain_id"])
    @append_keyword_filter(["file_id", "name"])
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',  # required
                'workspace_id': 'str',
                'domain_id': 'str'(meta),                                # required
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.file_mgr.stat_files(query)

    @staticmethod
    def _get_file_type(file_name: str) -> Union[str, None]:
        file_name_split = file_name.split(".")
        if len(file_name_split) == 1:
            return None
        else:
            return file_name_split[-1]
