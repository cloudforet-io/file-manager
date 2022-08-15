import logging
import functools
from spaceone.api.file_manager.v1 import file_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.file_manager.model.file_model import File, FileReference


__all__ = ['FileInfo', 'FilesInfo']
_LOGGER = logging.getLogger(__name__)


def FileReferenceInfo(reference_vo: FileReference):
    if reference_vo:
        info = {
            'resource_type': reference_vo.resource_type,
            'resource_id': reference_vo.resource_id
        }

        return file_pb2.FileReference(**info)
    else:
        return None


def FileInfo(file_vo: File, minimal=False, upload_url=None, upload_options=None, download_url=None):
    info = {
        'file_id': file_vo.file_id,
        'name': file_vo.name,
        'state': file_vo.state,
        'reference': file_vo.reference
    }

    if minimal is False:
        info.update({
            'scope': file_vo.scope,
            'file_type': file_vo.file_type,
            'tags': change_struct_type(file_vo.tags),
            'reference': FileReferenceInfo(file_vo.reference),
            'project_id': file_vo.project_id,
            'domain_id': file_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(file_vo.created_at)
        })

        if upload_url:
            info['upload_url'] = upload_url
            info['upload_options'] = change_struct_type(upload_options) if upload_options else None

        if download_url:
            info['download_url'] = download_url

    return file_pb2.FileInfo(**info)


def FilesInfo(file_vos, total_count, **kwargs):
    results = list(map(functools.partial(FileInfo, **kwargs), file_vos))

    return file_pb2.FilesInfo(results=results, total_count=total_count)
