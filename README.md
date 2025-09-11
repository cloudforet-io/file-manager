# File Manager

This document outlines the configuration settings for the **REST API for File Manager** application.

## Configuration



# Database
Database SettingsThe application uses MongoDB as the default database. Auto-creation of indexes is enabled.

DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    "default": {
        "db": "",
        "host": "",
        "port": 27017,
        "username": "",
        "password": "",
    }
}

#Cache Settings

Cache SettingsCaching is configured with a local cache backend.

CACHES = {
    "default": {},
    "local": {
        "backend": "spaceone.core.cache.local_cache.LocalCache",
        "max_size": 128,
        "ttl": 300,
    },
}

# Handlers
The application defines handlers for authentication, authorization, and mutation processes.

HANDLERS = {
    "authentication": [
        {
            "backend": "spaceone.core.handler.authentication_handler:SpaceONEAuthenticationHandler"
        }
    ], 
    "authorization": [
        {
            "backend": "spaceone.core.handler.authorization_handler:SpaceONEAuthorizationHandler"
        }
    ], 
    "mutation": [
        {
            "backend": "spaceone.core.handler.mutation_handler:SpaceONEMutationHandler"
        }
    ]
}

# Backend Connector
supported connectors is GCPGCSConnector,AWSS3Connector,MinIOS3Connector.

BACKEND = [GCPGCSConnector,AWSS3Connector,MinIOS3Connector]

# Connectors
The application supports multiple connectors for file storage and management.

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
        "endpoint": "<required>",
        "minio_access_key_id": "<optional>",
        "minio_secret_access_key": "<optional>",
        "region_name": "<required>",
        "bucket_name": "<required>",
    },
    "GCPGCSConnector": {
        "backend": "spaceone.file_manager.connector.gcp_gcs_connector:GCPGCSConnector",
        "project_id": "<required>",
        "bucket_name": "<required>",
        "service_account_key": "service_account_key is a base64-encoded string",
    },
    "SpaceConnector": {
        "backend": "spaceone.core.connector.space_connector:SpaceConnector",
        "endpoints": {
            "identity": "grpc://localhost:50051",
        },
    },
}

# System Token and File Manager URL
System token and File Manager URL settings are defined as follows:

# System Token Settings
TOKEN = ""
FILE_MANAGER_URL = ""


#Notes
Replace placeholder values (e.g., <required>, <optional>) with appropriate values for your environment.
Ensure that the required fields for the selected backend connector are properly configured.
The SpaceConnector is used for internal service communication, with the identity endpoint set to grpc://localhost:50051.

For further details or contributions, please refer to the repository or contact the maintainers.


