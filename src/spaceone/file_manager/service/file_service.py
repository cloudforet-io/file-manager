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
    service = "file_manager"
    resource = "File"
    permission_group = "COMPOUND"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_mgr: FileManager = self.locator.get_manager(FileManager)
        self.identity_mgr: IdentityManager = self.locator.get_manager(IdentityManager)

    @transaction(scope="workspace_member:write")
    @check_required(["name", "permission_group", "domain_id"])
    def add(self, params: dict) -> Tuple[File, str, dict]:
        """Add file

        Args:
            params (dict): {
                'name': 'str',              # required
                'tags': 'dict',
                'reference': 'dict',
                'permission_group',         # required
                'workspace_id': 'str',
                'domain_id': 'str'(meta)    # required
            }

        Returns:
            file_vo
        """

        params["file_type"] = self._get_file_type(params["name"])

        if params["permission_group"] == "WORKSPACE":
            self.identity_mgr.get_workspace(params["workspace_id"], params["domain_id"])
        else:
            params["workspace_id"] = "*"

        file_vo: File = self.file_mgr.create_file(params)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager(
            FileConnectorManager
        )
        upload_url, upload_options = file_conn_mgr.get_upload_url(
            file_vo.file_id, file_vo.name
        )

        return file_vo, upload_url, upload_options

    @transaction(scope="workspace_member:write")
    @check_required(["file_id", "domain_id"])
    def update(self, params: dict) -> File:
        """Update file

        Args:
            params (dict): {
                'file_id': 'str',            # required
                'tags': 'dict',
                'reference': 'dict',
                'workspace_id': 'str',
                'domain_id': 'str'(meta)     # required
            }

        Returns:
            file_vo
        """

        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        file_id = params["file_id"]
        file_vo: File = self.file_mgr.get_file(file_id, workspace_id, domain_id)
        file_vo = self.file_mgr.update_file_by_vo(params, file_vo)

        return file_vo

    @transaction(scope="workspace_member:write")
    @check_required(["file_id", "domain_id"])
    def delete(self, params: dict) -> None:
        """Delete file

        Args:
            params (dict): {
                'file_id': 'str',           # required
                'workspace_id': 'str',
                'domain_id': 'str'(meta),   # required
            }

        Returns:
            None
        """

        file_id = params["file_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        file_vo: File = self.file_mgr.get_file(file_id, workspace_id, domain_id)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager(
            FileConnectorManager
        )
        file_conn_mgr.delete_file(file_id, file_vo.name)

        self.file_mgr.delete_file_by_vo(file_vo)

    @transaction(scope="workspace_member:read")
    @check_required(["file_id", "domain_id"])
    def get_download_url(self, params: dict) -> Tuple[File, Union[str, None]]:
        """Get download url of file

        Args:
            params (dict): {
                'file_id': 'str',         # required
                'workspace_id': 'str',
                'domain_id': 'str',       # required
            }

        Returns:
            file_data (dict)
        """

        file_id = params["file_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        file_vo: File = self.file_mgr.get_file(file_id, workspace_id, domain_id)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager(
            FileConnectorManager
        )

        if file_vo.state == "PENDING":
            if not file_conn_mgr.check_file(file_id, file_vo.name):
                raise ERROR_FILE_UPLOAD_STATE()

            file_vo = self.file_mgr.update_file_by_vo({"state": "DONE"}, file_vo)

        download_url = file_conn_mgr.get_download_url(
            file_id, file_vo.name, file_vo.domain_id
        )

        return file_vo, download_url

    @transaction(scope="workspace_member:read")
    @check_required(["file_id"])
    def get(self, params: dict) -> File:
        """Get file

        Args:
            params (dict): {
                'file_id': 'str',          # required
                'workspace_id': 'str',
                'domain_id': 'str'(meta)   # required
            }

        Returns:
            file_vo
        """

        file_id = params["file_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        return self.file_mgr.get_file(file_id, workspace_id, domain_id)

    @transaction(scope="workspace_member:read")
    @append_query_filter(
        [
            "file_id",
            "name",
            "state",
            "file_type",
            "resource_type",
            "resource_id",
            "permission_group",
            "workspace_id",
            "domain_id",
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
                'permission_group': 'str',
                'workspace_id': 'str',
                'domain_id': 'str',                             # required
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.file_mgr.list_files(query)

    @transaction(scope="workspace_member:read")
    @check_required(["query"])
    @append_query_filter(["domain_id"])
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
