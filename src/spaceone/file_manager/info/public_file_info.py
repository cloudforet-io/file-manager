import logging
import functools
from typing import Union
from spaceone.api.file_manager.v1 import public_file_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.file_manager.model.public_file_model import (
    PublicFile,
    PublicFileReference,
)

__all__ = ["PublicFileInfo", "PublicFilesInfo"]
_LOGGER = logging.getLogger(__name__)


def PublicFileReferenceInfo(
    reference_vo: PublicFileReference,
) -> Union[public_file_pb2.PublicFileReference, None]:
    if reference_vo:
        info = {
            "resource_type": reference_vo.resource_type,
            "resource_id": reference_vo.resource_id,
        }

        return public_file_pb2.PublicFileReference(**info)
    else:
        return None


def PublicFileInfo(
    public_file_vo: PublicFile,
    minimal: bool = False,
    upload_url: str = None,
    upload_options: str = None,
    download_url: str = None,
) -> public_file_pb2.PublicFileInfo:
    info = {
        "public_file_id": public_file_vo.public_file_id,
        "name": public_file_vo.name,
        "state": public_file_vo.state,
        "reference": public_file_vo.reference,
    }

    if minimal is False:
        info.update(
            {
                "file_type": public_file_vo.file_type,
                "tags": change_struct_type(public_file_vo.tags),
                "reference": PublicFileReferenceInfo(public_file_vo.reference),
                "created_at": utils.datetime_to_iso8601(public_file_vo.created_at),
            }
        )

        if upload_url:
            info["upload_url"] = upload_url
            info["upload_options"] = (
                change_struct_type(upload_options) if upload_options else None
            )

        if download_url:
            info["download_url"] = download_url

    return public_file_pb2.PublicFileInfo(**info)


def PublicFilesInfo(file_vos, total_count, **kwargs):
    results = list(map(functools.partial(PublicFileInfo, **kwargs), file_vos))

    return public_file_pb2.PublicFilesInfo(results=results, total_count=total_count)
