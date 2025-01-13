REST_TITLE = "REST API for File Manager"

DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    "default": {
        "db": "dev2-file-manager",
        "host": "localhost",
        "port": 27017,
        "username": "",
        "password": "",
    }
}

CACHES = {
    "default": {},
    "local": {
        "backend": "spaceone.core.cache.local_cache.LocalCache",
        "max_size": 128,
        "ttl": 300,
    },
}

HANDLERS = {}

# BACKEND = "AWSS3Connector"
BACKEND = "MinIOS3Connector"
CONNECTORS = {
    "AWSS3Connector": {
        "backend": "spaceone.file_manager.connector.aws_s3_connector:AWSS3Connector",
        "aws_access_key_id": "<optional>",
        "aws_secret_access_key": "<optional>",
        "region_name": "<required>",
        "bucket_name": "<required>",
    },
    "MinIOS3Connector": {
        "backend": "spaceone.file_manager.connector.minio_connector:MinIOS3Connector",
        "endpoint":"<required>",
        "minio_access_key_id": "<optional>",
        "minio_secret_access_key": "<optional>",
        "region_name": "<required>",
        "bucket_name": "<required>",
    },
    "SpaceConnector": {
        "backend": "spaceone.core.connector.space_connector:SpaceConnector",
        "endpoints": {
            "identity": "grpc://identity:50051",
        },
    },
}

# System Token Settings
TOKEN = ""
FILE_MANAGER_URL = ""
