from datetime import datetime
from typing import Union, List
from pydantic import BaseModel

from spaceone.core import utils

__all__ = ["UserFileResponse", "UserFilesResponse"]


class UserFileResponse(BaseModel):
    file_id: Union[str, None] = None
    name: Union[str, None] = None
    download_url: Union[str, None] = None
    reference: Union[dict, None] = None
    tags: Union[dict, None] = None
    domain_id: Union[str, None] = None
    user_id: Union[str, None] = None
    created_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        return data


class UserFilesResponse(BaseModel):
    results: List[UserFileResponse]
    total_count: int
