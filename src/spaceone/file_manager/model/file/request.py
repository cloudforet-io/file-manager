from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "FileAddRequest",
    "FileUpdateRequest",
    "FileDeleteRequest",
    "FileGetRequest",
    "FileSearchQueryRequest",
    "FileStatQueryRequest",
    "ResourceGroup",
]

ResourceGroup = Literal["SYSTEM", "DOMAIN", "WORKSPACE"]


class FileAddRequest(BaseModel):
    name: str
    file_type: Union[str, None] = None
    reference: Union[dict, None] = None
    file_binary: Union[bytes, None] = None
    tags: Union[dict, None] = None
    resource_group: ResourceGroup
    domain_id: Union[str, None] = None
    workspace_id: Union[str, None] = None


class FileUpdateRequest(BaseModel):
    file_id: str
    reference: Union[dict, None] = None
    tags: Union[dict, None] = None
    domain_id: Union[str, None] = None
    workspace_id: Union[str, None] = None


class FileDeleteRequest(BaseModel):
    file_id: str
    domain_id: Union[str, None] = None
    workspace_id: Union[str, None] = None


class FileGetRequest(BaseModel):
    file_id: str
    domain_id: Union[list, str, None] = None
    workspace_id: Union[list, str, None] = None


class FileSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    file_id: Union[str, None] = None
    name: Union[str, None] = None
    file_type: Union[str, None] = None
    resource_type: Union[str, None] = None
    resource_id: Union[str, None] = None
    domain_id: Union[list, str, None] = None
    workspace_id: Union[list, str, None] = None


class FileStatQueryRequest(BaseModel):
    query: dict
    domain_id: Union[list, str, None] = None
    workspace_id: Union[list, str, None] = None
