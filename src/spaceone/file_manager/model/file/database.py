import logging
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
    file_type = StringField(max_length=40, null=True, default=None)
    name = StringField(max_length=255, required=True)
    download_url = StringField(null=True, default=None)
    tags = DictField()
    reference = EmbeddedDocumentField(FileReference, null=True, default=None)
    resource_group = StringField(
        max_length=40, choices=("SYSTEM", "DOMAIN", "WORKSPACE", "PROJECT")
    )
    workspace_id = StringField(max_length=40, null=True, default=None)
    domain_id = StringField(max_length=40, null=True, default=None)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        "updatable_fields": ["download_url", "tags", "reference"],
        "minimal_fields": [
            "file_id",
            "name",
            "reference",
            "resource_group",
            "workspace_id",
            "domain_id",
        ],
        "change_query_keys": {
            "resource_type": "reference.resource_type",
            "resource_id": "reference.resource_id",
        },
        "ordering": ["name"],
        "indexes": [
            "file_id",
            "reference.resource_type",
            "reference.resource_id",
            "resource_group",
            "workspace_id",
            "domain_id",
        ],
    }
