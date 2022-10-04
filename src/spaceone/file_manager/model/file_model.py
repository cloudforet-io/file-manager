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
    file_id = StringField(max_length=40, generate_id='file', unique=True)
    name = StringField(max_length=255, required=True)
    state = StringField(max_length=40, choices=('PENDING', 'DONE'), default='PENDING')
    scope = StringField(max_length=40, choices=('PUBLIC', 'DOMAIN'), required=True)
    file_type = StringField(max_length=255, null=True, default=None)
    tags = DictField()
    reference = EmbeddedDocumentField(FileReference, null=True, default=None)
    domain_id = StringField(max_length=40, null=True, default=None)
    user_id = StringField(max_length=40)
    user_domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'state',
            'tags',
            'reference'
        ],
        'minimal_fields': [
            'file_id',
            'name',
            'state',
            'reference'
        ],
        'change_query_keys': {
            'resource_type': 'reference.resource_type',
            'resource_id': 'reference.resource_id',
            'user_domains': 'domain_id'
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'state',
            'scope',
            'file_type',
            'reference.resource_type',
            'reference.resource_id',
            'domain_id',
            'user_id',
            'user_domain_id'
        ]
    }
