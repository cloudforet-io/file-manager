import logging

from spaceone.core import cache
from spaceone.core.service import *
from spaceone.file_manager.error import *
from spaceone.file_manager.model.file_model import File
from spaceone.file_manager.manager.identity_manager import IdentityManager
from spaceone.file_manager.manager.file_manager import FileManager
from spaceone.file_manager.manager.file_connector_manager import FileConnectorManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class FileService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_mgr: FileManager = self.locator.get_manager('FileManager')

    @transaction(append_meta={
        'authorization.scope': 'DOMAIN_OR_PROJECT',
        'authorization.require_project_id': True
    })
    @check_required(['name', 'domain_id'])
    def add(self, params):
        """ Add file

        Args:
            params (dict): {
                'name': 'str',
                'tags': 'dict',
                'reference': 'dict',
                'project_id': 'str',
                'domain_id': 'str
            }

        Returns:
            file_vo
        """

        project_id = params.get('project_id')
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        if project_id:
            identity_mgr.get_project(project_id, domain_id)
            params['scope'] = 'PROJECT'
        else:
            params['scope'] = 'GLOBAL'

        params['file_type'] = self._get_file_type(params['name'])

        file_vo: File = self.file_mgr.create_file(params)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager('FileConnectorManager')
        upload_url, upload_options = file_conn_mgr.get_upload_url(file_vo.file_id, file_vo.name)

        return file_vo, upload_url, upload_options

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_PROJECT'})
    @check_required(['file_id', 'domain_id'])
    def update(self, params):
        """ Update file

        Args:
            params (dict): {
                'file_id': 'str',
                'tags': 'dict',
                'reference': 'dict',
                'project_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            file_vo
        """

        domain_id = params['domain_id']
        file_id = params['file_id']
        project_id = params.get('project_id')

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        file_vo: File = self.file_mgr.get_file(file_id, domain_id)

        if project_id:
            if file_vo.scope == 'GLOBAL':
                raise ERROR_CHANGE_PROJECT()
            else:
                identity_mgr.get_project(project_id, domain_id)

        file_vo = self.file_mgr.update_file_by_vo(params, file_vo)

        return file_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_PROJECT'})
    @check_required(['file_id', 'domain_id'])
    def delete(self, params):
        """ Delete file

        Args:
            params (dict): {
                'file_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        domain_id = params['domain_id']
        file_id = params['file_id']

        file_vo: File = self.file_mgr.get_file(file_id, domain_id)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager('FileConnectorManager')
        file_conn_mgr.delete_file(file_id, file_vo.name)

        self.file_mgr.delete_file_by_vo(file_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_PROJECT'})
    @check_required(['file_id', 'domain_id'])
    def get_download_url(self, params):
        """ Get download url of file

        Args:
            params (dict): {
                'file_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            file_data (dict)
        """

        domain_id = params['domain_id']
        file_id = params['file_id']

        file_vo: File = self.file_mgr.get_file(file_id, domain_id)

        file_conn_mgr: FileConnectorManager = self.locator.get_manager('FileConnectorManager')

        if file_vo.state == 'PENDING':
            if not file_conn_mgr.check_file(file_id, file_vo.name):
                raise ERROR_FILE_UPLOAD_STATE()

            file_vo = self.file_mgr.update_file_by_vo({'state': 'DONE'}, file_vo)

        download_url = file_conn_mgr.get_download_url(file_id, file_vo.name, domain_id)

        return file_vo, download_url

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_PROJECT'})
    @check_required(['file_id', 'domain_id'])
    def get(self, params):
        """ Get file

        Args:
            params (dict): {
                'file_id': 'str',
                'domain_id': 'str',
                'only': 'list'
            }

        Returns:
            file_vo
        """

        return self.file_mgr.get_file(params['file_id'], params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['file_id', 'name', 'state', 'scope', 'file_type', 'resource_type', 'resource_id',
                          'project_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(['file_id', 'name'])
    def list(self, params):
        """ List files

        Args:
            params (dict): {
                'file_id': 'str',
                'name': 'str',
                'state': 'str',
                'scope': 'str',
                'file_type': 'str',
                'resource_type': 'str',
                'resource_id': 'str',
                'project_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get('query', {})
        return self.file_mgr.list_files(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['file_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.file_mgr.stat_files(query)

    @staticmethod
    def _get_file_type(file_name):
        file_name_split = file_name.split('.')
        if len(file_name_split) == 1:
            return None
        else:
            return file_name_split[-1]
