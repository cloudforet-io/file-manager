import logging

from spaceone.core.manager import BaseManager
from spaceone.file_manager.model.file_model import File

_LOGGER = logging.getLogger(__name__)


class FileManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_model: File = self.locator.get_model(File)

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

    def get_file(self, file_id: str, workspace_id: str, domain_id: str) -> File:
        return self.file_model.get(
            file_id=file_id, workspace_id=workspace_id, domain_id=domain_id
        )

    def list_files(self, query: dict) -> dict:
        return self.file_model.query(**query)

    def stat_files(self, query: dict) -> dict:
        return self.file_model.stat(**query)
