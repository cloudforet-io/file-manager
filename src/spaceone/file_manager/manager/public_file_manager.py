import logging

from spaceone.core.manager import BaseManager
from spaceone.file_manager.model.public_file_model import PublicFile

_LOGGER = logging.getLogger(__name__)


class PublicFileManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.public_file_model: PublicFile = self.locator.get_model(PublicFile)

    def create_file(self, params: dict) -> PublicFile:
        def _rollback(vo: PublicFile) -> None:
            _LOGGER.info(f"[ROLLBACK] Delete file : {vo.name} ({vo.public_file_id})")
            vo.delete()

        public_file_vo: PublicFile = self.public_file_model.create(params)

        self.transaction.add_rollback(_rollback, public_file_vo)

        return public_file_vo

    def update_file_by_vo(self, params: dict, public_file_vo: PublicFile) -> PublicFile:
        def _rollback(old_data: dict):
            _LOGGER.info(
                f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["public_file_id"]})'
            )
            public_file_vo.update(old_data)

        self.transaction.add_rollback(_rollback, public_file_vo.to_dict())

        return public_file_vo.update(params)

    @staticmethod
    def delete_file_by_vo(public_file_vo: PublicFile) -> None:
        public_file_vo.delete()

    def get_file(self, public_file_id: str) -> PublicFile:
        return self.public_file_model.get(public_file_id=public_file_id)

    def list_files(self, query: dict) -> dict:
        return self.public_file_model.query(**query)

    def stat_files(self, query: dict) -> dict:
        return self.public_file_model.stat(**query)
