from spaceone.core.pygrpc.server import GRPCServer
from spaceone.file_manager.interface.grpc.file import File
from spaceone.file_manager.interface.grpc.public_file import PublicFile

_all_ = ["app"]

app = GRPCServer()
app.add_service(File)
app.add_service(PublicFile)
