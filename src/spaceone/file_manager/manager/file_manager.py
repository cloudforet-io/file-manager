import logging
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.file_manager.model.file.database import File

_LOGGER = logging.getLogger(__name__)


class FileManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_model = File

    def create_file(self, params: dict) -> File:
        def _rollback(vo: File) -> None:
            _LOGGER.info(f"[ROLLBACK] Delete file : {vo.name} ({vo.file_id})")
            vo.delete()

        file_vo: File = self.file_model.create(params)
        self.transaction.add_rollback(_rollback, file_vo)

        return file_vo

    def update_file_by_vo(self, params: dict, file_vo: File) -> File:
        def _rollback(old_data: dict):
            _LOGGER.info(
                f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["file_id"]})'
            )
            file_vo.update(old_data)

        self.transaction.add_rollback(_rollback, file_vo.to_dict())

        return file_vo.update(params)

    @staticmethod
    def delete_file_by_vo(file_vo: File) -> None:
        file_vo.delete()

    def get_file(
        self,
        file_id: str,
        domain_id: str = None,
        workspace_id: str = None,
    ) -> File:
        conditions = {"file_id": file_id}

        if domain_id:
            conditions["domain_id"] = domain_id

        if workspace_id:
            conditions["workspace_id"] = workspace_id
            
        return self.file_model.get(**conditions)

    def filter_files(self, **conditions) -> QuerySet:
        return self.file_model.filter(**conditions)

    def list_files(self, query: dict) -> dict:
        return self.file_model.query(**query)

    def stat_files(self, query: dict) -> dict:
        return self.file_model.stat(**query)
