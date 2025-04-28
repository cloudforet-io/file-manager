import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_connector: SpaceConnector = self.locator.get_connector(
            SpaceConnector, service="identity", timeout=30
        )

    def check_workspace(self, workspace_id, domain_id):
        system_token = config.get_global("TOKEN")
        return self.identity_connector.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
            token=system_token,
        )

    def get_project(self, project_id, domain_id):
        system_token = config.get_global("TOKEN")
        return self.identity_connector.dispatch(
            "Project.get",
            {"project_id": project_id},
            x_domain_id=domain_id,
            token=system_token,
        )
