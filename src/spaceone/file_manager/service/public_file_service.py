import logging
from typing import Tuple, Union

from spaceone.core.service import *
from spaceone.file_manager.error import *
from spaceone.file_manager.model.public_file_model import PublicFile
from spaceone.file_manager.manager.public_file_manager import PublicFileManager
from spaceone.file_manager.manager.public_file_connector_manager import (
    PublicFileConnectorManager,
)

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PublicFileService(BaseService):
    service = "file_manager"
    resource = "PublicFile"
    permission_group = "GLOBAL"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.public_file_mgr: PublicFileManager = self.locator.get_manager(
            PublicFileManager
        )

    @transaction(scope="workspace_member:write")
    @check_required(["name"])
    def add(self, params: dict) -> Tuple[PublicFile, str, dict]:
        """Add file

        Args:
            params (dict): {
                'name': 'str',          # required
                'tags': 'dict',
                'reference': 'dict'
            }

        Returns:
            public_file_vo
        """

        params["file_type"] = self._get_file_type(params["name"])

        public_file_vo: PublicFile = self.public_file_mgr.create_file(params)

        public_file_conn_mgr: PublicFileConnectorManager = self.locator.get_manager(
            PublicFileConnectorManager
        )
        upload_url, upload_options = public_file_conn_mgr.get_upload_url(
            public_file_vo.public_file_id, public_file_vo.name
        )

        return public_file_vo, upload_url, upload_options

    @transaction(scope="workspace_member:write")
    @check_required(["public_file_id"])
    def update(self, params: dict) -> PublicFile:
        """Update file

        Args:
            params (dict): {
                'public_file_id': 'str',   # required
                'tags': 'dict',
                'reference': 'dict'
            }

        Returns:
            public_file_vo
        """

        public_file_id = params["public_file_id"]
        public_file_vo: PublicFile = self.public_file_mgr.get_file(public_file_id)
        public_file_vo = self.public_file_mgr.update_file_by_vo(params, public_file_vo)

        return public_file_vo

    @transaction(scope="workspace_member:write")
    @check_required(["public_file_id"])
    def delete(self, params: dict) -> None:
        """Delete file

        Args:
            params (dict): {
                'public_file_id': 'str'   # required
            }

        Returns:
            None
        """

        public_file_id = params["public_file_id"]
        public_file_vo: PublicFile = self.public_file_mgr.get_file(public_file_id)

        public_file_conn_mgr: PublicFileConnectorManager = self.locator.get_manager(
            PublicFileConnectorManager
        )
        public_file_conn_mgr.delete_file(public_file_id, public_file_vo.name)

        self.public_file_mgr.delete_file_by_vo(public_file_vo)

    @transaction(scope="workspace_member:read")
    @check_required(["public_file_id"])
    def get_download_url(self, params: dict) -> Tuple[PublicFile, Union[str, None]]:
        """Get download url of file

        Args:
            params (dict): {
                'public_file_id': 'str'    # required
            }

        Returns:
            file_data (dict)
        """

        public_file_id = params["public_file_id"]
        public_file_vo: PublicFile = self.public_file_mgr.get_file(public_file_id)

        public_file_conn_mgr: PublicFileConnectorManager = self.locator.get_manager(
            PublicFileConnectorManager
        )

        if public_file_vo.state == "PENDING":
            if not public_file_conn_mgr.check_file(public_file_id, public_file_vo.name):
                raise ERROR_FILE_UPLOAD_STATE()

            public_file_vo = self.public_file_mgr.update_file_by_vo(
                {"state": "DONE"}, public_file_vo
            )

        download_url = public_file_conn_mgr.get_download_url(
            public_file_id, public_file_vo.name
        )

        return public_file_vo, download_url

    @transaction(scope="workspace_member:read")
    @check_required(["public_file_id"])
    def get(self, params: dict) -> PublicFile:
        """Get file

        Args:
            params (dict): {
                'public_file_id': 'str'    # required
            }

        Returns:
            public_file_vo
        """

        public_file_id = params["public_file_id"]

        return self.public_file_mgr.get_file(public_file_id)

    @transaction(scope="workspace_member:read")
    @append_query_filter(
        ["public_file_id", "name", "state", "file_type", "resource_type", "resource_id"]
    )
    @append_keyword_filter(["public_file_id", "name"])
    def list(self, params: dict) -> dict:
        """List files

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'public_file_id': 'str',
                'name': 'str',
                'state': 'str',
                'file_type': 'str',
                'resource_type': 'str',
                'resource_id': 'str'
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.public_file_mgr.list_files(query)

    @transaction(scope="workspace_member:read")
    @check_required(["query"])
    @append_keyword_filter(["public_file_id", "name"])
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'   # required
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.public_file_mgr.stat_files(query)

    @staticmethod
    def _get_file_type(file_name: str) -> Union[str, None]:
        file_name_split = file_name.split(".")
        if len(file_name_split) == 1:
            return None
        else:
            return file_name_split[-1]
