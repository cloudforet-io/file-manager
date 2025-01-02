from spaceone.core.error import *


class ERROR_NOT_AUTHORIZATION_USER(ERROR_BASE):
    _message = "Not authorization user. (user_id = {user_id}, domain_id = {domain_id})"

class  ERROR_NOT_SUPPORTED_RESOURCE_GROUP(ERROR_BASE):
    _message = "Not supported resource group. (resource_group = {resource_group})"

class ERROR_NOT_DEFINED_FILE_BACKEND(ERROR_BASE):
    _message = "File backend not defined. (backend = {backend})"

class ERROR_FILE_DOWNLOAD_URL_EXIST(ERROR_BASE):
    _message = "File download url is not exist. (file_id = {file_id}, name = {name})"


class ERROR_FILE_DOWNLOAD_FAILED(ERROR_BASE):
    _message = "File download failed. (name = {name})"
    
class ERROR_NOT_MATCH_USER_ID(ERROR_BASE):
    _message = "Not match user_id. (user_id = {user_id}, domain_id = {domain_id})"
    
class ERROR_FILE_UPLOAD_FAILED(ERROR_BASE):
    _message = "File upload failed. (name = {name})"
    
    
class ERROR_FILE_DELETE_FAILED(ERROR_BASE):
    _message = "File delete failed. (name = {name})"