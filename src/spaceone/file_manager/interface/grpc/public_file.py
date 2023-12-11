from spaceone.api.file_manager.v1 import public_file_pb2, public_file_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.file_manager.service.public_file_service import PublicFileService
from spaceone.file_manager.info.public_file_info import *
from spaceone.file_manager.info.common_info import *


class PublicFile(BaseAPI, public_file_pb2_grpc.PublicFileServicer):
    pb2 = public_file_pb2
    pb2_grpc = public_file_pb2_grpc

    def add(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            public_file_vo, upload_url, upload_options = public_file_service.add(params)
            return self.locator.get_info(
                PublicFileInfo,
                public_file_vo,
                upload_url=upload_url,
                upload_options=upload_options,
            )

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            return self.locator.get_info(
                PublicFileInfo, public_file_service.update(params)
            )

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            public_file_service.delete(params)
            return self.locator.get_info(EmptyInfo)

    def get_download_url(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            public_file_vo, download_url = public_file_service.get_download_url(params)
            return self.locator.get_info(
                PublicFileInfo, public_file_vo, download_url=download_url
            )

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            return self.locator.get_info(
                PublicFileInfo, public_file_service.get(params)
            )

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            public_file_vos, total_count = public_file_service.list(params)
            return self.locator.get_info(
                PublicFilesInfo,
                public_file_vos,
                total_count,
                minimal=self.get_minimal(params),
            )

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            PublicFileService, metadata
        ) as public_file_service:
            return self.locator.get_info(
                StatisticsInfo, public_file_service.stat(params)
            )
