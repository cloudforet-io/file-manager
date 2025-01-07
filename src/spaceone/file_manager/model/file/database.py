import logging
from fastapi.utils import generate_unique_id
from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel

_LOGGER = logging.getLogger(__name__)


class FileReference(EmbeddedDocument):
    resource_type = StringField(required=True)
    resource_id = StringField(required=True)

    def to_dict(self):
        return dict(self.to_mongo())


class File(MongoModel):
    file_id = StringField(max_length=40, generate_id="file", unique=True)
    name = StringField(max_length=255, required=True)
    tags = DictField()
    reference = EmbeddedDocumentField(FileReference, null=True, default=None)
    resource_group = StringField(
        max_length=40, choices=("SYSTEM", "DOMAIN", "WORKSPACE", "PROJECT")
    )
    domain_id = StringField(max_length=40, null=True, default=None)
    workspace_id = StringField(max_length=40, null=True, default=None)
    project_id = StringField(max_length=40, null=True, default=None)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        "updatable_fields": ["tags", "reference", "project_id"],
        "minimal_fields": [
            "file_id",
            "name",
            "reference",
            "resource_group",
            "domain_id",
            "workspace_id",
            "project_id",
        ],
        "change_query_keys": {
            "resource_type": "reference.resource_type",
            "resource_id": "reference.resource_id",
            "user_projects": "project_id",
        },
        "ordering": ["name"],
        "indexes": [
            "reference.resource_type",
            "reference.resource_id",
            "resource_group",
            "domain_id",
            "workspace_id",
            "project_id",
        ],
    }
