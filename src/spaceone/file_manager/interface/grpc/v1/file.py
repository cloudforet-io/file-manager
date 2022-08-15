from spaceone.api.file_manager.v1 import file_pb2, file_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class File(BaseAPI, file_pb2_grpc.FileServicer):

    pb2 = file_pb2
    pb2_grpc = file_pb2_grpc

    def add(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            file_vo, upload_url, upload_options = file_service.add(params)
            return self.locator.get_info('FileInfo', file_vo, upload_url=upload_url, upload_options=upload_options)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            return self.locator.get_info('FileInfo', file_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            file_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get_download_url(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            file_vo, download_url = file_service.get_download_url(params)
            return self.locator.get_info('FileInfo', file_vo, download_url=download_url)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            return self.locator.get_info('FileInfo', file_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            file_vos, total_count = file_service.list(params)
            return self.locator.get_info('FilesInfo', file_vos, total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('FileService', metadata) as file_service:
            return self.locator.get_info('StatisticsInfo', file_service.stat(params))
