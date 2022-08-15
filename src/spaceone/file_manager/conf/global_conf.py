DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    'default': {
        'db': 'file-manager',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}

CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 300
    }
}

HANDLERS = {}

BACKEND = 'AWSS3Connector'
CONNECTORS = {
    'AWSS3Connector': {
        'backend': 'spaceone.file_manager.connector.aws_s3_connector.AWSS3Connector',
        'aws_access_key_id': '<optional>',
        'aws_secret_access_key': '<optional>',
        'region_name': '<required>',
        'bucket_name': '<required>',
    },
    'SpaceConnector': {
        'backend': 'spaceone.core.connector.space_connector.SpaceConnector',
        'endpoints': {
            'identity': 'grpc://identity:50051',
        }
    },
}
