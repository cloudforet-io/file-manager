import logging
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.file_manager.model.user_file.database import UserFile

_LOGGER = logging.getLogger(__name__)


class UserFileManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_file_model = UserFile

    def create_user_file(self, params: dict) -> UserFile:
        def _rollback(vo: UserFile) -> None:
            _LOGGER.info(f"[ROLLBACK] Delete user_file : {vo.name} ({vo.file_id})")
            vo.delete()

        user_file_vo: UserFile = self.user_file_model.create(params)
        self.transaction.add_rollback(_rollback, user_file_vo)

        return user_file_vo

    def update_user_file_by_vo(self, params: dict, user_file_vo: UserFile) -> UserFile:
        def _rollback(old_data: dict):
            _LOGGER.info(
                f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["user_file_id"]})'
            )
            user_file_vo.update(old_data)

        self.transaction.add_rollback(_rollback, user_file_vo.to_dict())

        return user_file_vo.update(params)

    @staticmethod
    def delete_user_file_by_vo(user_file_vo: UserFile) -> None:
        user_file_vo.delete()

    def get_user_file(
        self,
        file_id: str,
        domain_id: str,
        user_id: str,
    ) -> UserFile:

        return self.user_file_model.get(file_id=file_id, domain_id=domain_id, user_id=user_id)

    def filter_user_files(self, **conditions) -> QuerySet:
        return self.user_file_model.filter(**conditions)

    def list_user_files(self, query: dict) -> dict:
        return self.user_file_model.query(**query)

    def stat_user_files(self, query: dict) -> dict:
        return self.user_file_model.stat(**query)
