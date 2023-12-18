from spaceone.core.pygrpc.server import GRPCServer
from spaceone.file_manager.interface.grpc.file import File

_all_ = ["app"]

app = GRPCServer()
app.add_service(File)
