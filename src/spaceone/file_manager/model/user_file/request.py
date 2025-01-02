from typing import Union, Literal

from pydantic import BaseModel

__all__ = [
    "UserFileAddRequest",
    "UserFileUpdateRequest",
    "UserFileDeleteRequest",
    "UserFileGetRequest",
    "UserFileSearchQueryRequest",
    "UserFileStatQueryRequest",
]

class UserFileAddRequest(BaseModel):
    file_id: Union[str, None] = None
    name: str 
    domain_id: Union[str, None] = None
    user_id: Union[str, None] = None


class UserFileUpdateRequest(BaseModel):
    file_id: str
    reference: Union[dict, None] = None
    tags: Union[dict, None] = None
    domain_id: Union[str, None] = None
    download_url: Union[str, None] = None
    user_id: Union[str, None] = None


class UserFileDeleteRequest(BaseModel):
    file_id: str
    domain_id: Union[str, None] = None
    user_id: Union[str, None] = None


class UserFileGetRequest(BaseModel):
    file_id: str
    domain_id: Union[list, str, None] = None
    user_id: Union[str, None] = None


class UserFileSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    file_id: Union[str, None] = None
    name: Union[str, None] = None
    resource_type: Union[str, None] = None
    resource_id: Union[str, None] = None
    domain_id: Union[list, str, None] = None
    user_id: Union[str, None] = None


class UserFileStatQueryRequest(BaseModel):
    query: dict
    domain_id: Union[list, str, None] = None
    user_id: Union[str, None] = None
