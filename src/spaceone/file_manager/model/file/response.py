from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils
from spaceone.file_manager.model.file.request import ResourceGroup

__all__ = ["FileResponse", "FilesResponse"]


class FileResponse(BaseModel):
    file_id: Union[str, None] = None
    name: Union[str, None] = None
    download_url: Union[str, None] = None
    reference: Union[dict, None] = None
    tags: Union[dict, None] = None
    resource_group: Union[ResourceGroup, None] = None
    domain_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    project_id: Union[str, None] = None
    created_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        
        if data["resource_group"] == "SYSTEM":
            data["download_url"] = "/files/public/" + data["file_id"]
        elif data["resource_group"] == "DOMAIN":
            data["download_url"] = "/files/domain/" + data["file_id"]
        elif data["resource_group"] == "WORKSPACE":
            data["download_url"] = "/files/workspace/" + data["file_id"]
        elif data["resource_group"] == "PROJECT":
            data["download_url"] = "/files/project/" + data["file_id"]
        
        return data


class FilesResponse(BaseModel):
    results: List[FileResponse]
    total_count: int
