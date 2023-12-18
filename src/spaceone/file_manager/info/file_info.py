import logging
import functools
from typing import Union
from spaceone.api.file_manager.v1 import file_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.file_manager.model.file_model import File, FileReference

__all__ = ["FileInfo", "FilesInfo"]
_LOGGER = logging.getLogger(__name__)


def FileReferenceInfo(
    reference_vo: FileReference,
) -> Union[file_pb2.FileReference, None]:
    if reference_vo:
        info = {
            "resource_type": reference_vo.resource_type,
            "resource_id": reference_vo.resource_id,
        }

        return file_pb2.FileReference(**info)
    else:
        return None


def FileInfo(
    file_vo: File,
    minimal: bool = False,
    upload_url: str = None,
    upload_options: str = None,
    download_url: str = None,
) -> file_pb2.FileInfo:
    info = {
        "file_id": file_vo.file_id,
        "name": file_vo.name,
        "state": file_vo.state,
        "resource_group": file_vo.resource_group,
        "workspace_id": file_vo.workspace_id,
        "domain_id": file_vo.domain_id,
    }

    if minimal is False:
        info.update(
            {
                "file_type": file_vo.file_type,
                "tags": change_struct_type(file_vo.tags),
                "reference": FileReferenceInfo(file_vo.reference),
                "created_at": utils.datetime_to_iso8601(file_vo.created_at),
            }
        )

        if upload_url:
            info["upload_url"] = upload_url
            info["upload_options"] = (
                change_struct_type(upload_options) if upload_options else None
            )

        if download_url:
            info["download_url"] = download_url

    return file_pb2.FileInfo(**info)


def FilesInfo(file_vos, total_count, **kwargs):
    results = list(map(functools.partial(FileInfo, **kwargs), file_vos))

    return file_pb2.FilesInfo(results=results, total_count=total_count)
