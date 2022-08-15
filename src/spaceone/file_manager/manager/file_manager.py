import logging

from spaceone.core.manager import BaseManager
from spaceone.file_manager.model.file_model import File

_LOGGER = logging.getLogger(__name__)


class FileManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_model: File = self.locator.get_model('File')

    def create_file(self, params):
        def _rollback(file_vo):
            _LOGGER.info(f'[ROLLBACK] Delete file : {file_vo.name} ({file_vo.file_id})')
            file_vo.delete()

        file_vo: File = self.file_model.create(params)

        self.transaction.add_rollback(_rollback, file_vo)

        return file_vo

    def update_file(self, params):
        self.update_file_by_vo(params, self.get_file(params['file_id'], params['domain_id']))

    def update_file_by_vo(self, params, file_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["file_id"]})')
            file_vo.update(old_data)

        self.transaction.add_rollback(_rollback, file_vo.to_dict())

        return file_vo.update(params)

    def delete_file(self, file_id, domain_id):
        self.delete_file_by_vo(self.get_file(file_id, domain_id))

    @staticmethod
    def delete_file_by_vo(file_vo):
        file_vo.delete()

    def get_file(self, file_id, domain_id, only=None):
        return self.file_model.get(file_id=file_id, domain_id=domain_id, only=only)

    def filter_files(self, **conditions):
        return self.file_model.filter(**conditions)

    def list_files(self, query):
        return self.file_model.query(**query)

    def stat_files(self, query):
        return self.file_model.stat(**query)
