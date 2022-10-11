from spaceone.core.error import *


class ERROR_NOT_DEFINED_FILE_BACKEND(ERROR_BASE):
    _message = 'File backend not defined. (backend = {backend})'


class ERROR_FILE_UPLOAD_STATE(ERROR_INVALID_ARGUMENT):
    _message = 'The file has not been uploaded yet. (state = PENDING)'
