import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_connector: SpaceConnector = self.locator.get_connector(
            SpaceConnector, service="identity"
        )

    def check_workspace(self, workspace_id, domain_id):
        system_token = config.get_global("TOKEN")
        return self.identity_connector.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
            token=system_token,
        )

    # def check_project(self, project_id, domain_id):
    #     system_token = config.get_global("TOKEN")
    #     return self.identity_connector.dispatch(
    #         "Project.check",
    #         {"project_id": project_id, "domain_id": domain_id},
    #         token=system_token,
    #     )
    
    # def check_user(self, user_id, domain_id):
    #     system_token = config.get_global("TOKEN")
    #     return self.identity_connector.dispatch(
    #         "User.check",
    #         {"user_id": user_id, "domain_id": domain_id},
    #         token=system_token,
    #     )