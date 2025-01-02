from spaceone.core.pygrpc.server import GRPCServer
from spaceone.file_manager.interface.grpc.file import File
from spaceone.file_manager.interface.grpc.user_file import UserFile

_all_ = ["app"]

app = GRPCServer()
app.add_service(File)
app.add_service(UserFile)
