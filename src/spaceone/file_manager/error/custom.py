from spaceone.core.error import *


class  ERROR_NOT_SUPPORTED_RESOURCE_GROUP(ERROR_BASE):
    _message = "Not supported resource group. (resource_group = {resource_group})"

class ERROR_NOT_DEFINED_FILE_BACKEND(ERROR_BASE):
    _message = "File backend not defined. (backend = {backend})"

class ERROR_FILE_DOWNLOAD_URL_EXIST(ERROR_BASE):
    _message = "File download url is not exist. (file_id = {file_id})"


class ERROR_FILE_DOWNLOAD_FAILED(ERROR_BASE):
    _message = "File download failed. (file_id = {file_id})"