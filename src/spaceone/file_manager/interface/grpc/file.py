from spaceone.api.file_manager.v1 import file_pb2, file_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.file_manager.service.file_service import FileService


class File(BaseAPI, file_pb2_grpc.FileServicer):
    pb2 = file_pb2
    pb2_grpc = file_pb2_grpc

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        file_svc = FileService(metadata)
        response: dict = file_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        file_svc = FileService(metadata)
        file_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        file_svc = FileService(metadata)
        response: dict = file_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        file_svc = FileService(metadata)
        response: dict = file_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        file_svc = FileService(metadata)
        response: dict = file_svc.stat(params)
        return self.dict_to_message(response)
