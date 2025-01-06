from spaceone.api.file_manager.v1 import user_file_pb2, user_file_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.file_manager.service.user_file_service import UserFileService


class UserFile(BaseAPI, user_file_pb2_grpc.UserFileServicer):
    pb2 = user_file_pb2
    pb2_grpc = user_file_pb2_grpc

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        user_file_svc = UserFileService(metadata)
        response: dict = user_file_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        user_file_svc = UserFileService(metadata)
        user_file_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        user_file_svc = UserFileService(metadata)
        response: dict = user_file_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        user_file_svc = UserFileService(metadata)
        response: dict = user_file_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        user_file_svc = UserFileService(metadata)
        response: dict = user_file_svc.stat(params)
        return self.dict_to_message(response)
