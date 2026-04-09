from concurrent import futures

import grpc

"""
Example gRPC Server Usage:

import lemegeton.protocol.protobuf.template.template_pb2 as template_pb2
import lemegeton.protocol.protobuf.template.template_pb2_grpc as template_pb2_grpc
from lemegeton.protocol.protobuf.grpc_protocol import GRPCServer

if __name__ == "__main__":

    def callback(request):
        print("Received request:", request.name, request.age)
        return template_pb2.RESPONSE(
            message=f"Hello {request.name}, you are {request.age} years old!"
        )

    server = GRPCServer(
        service_module=template_pb2_grpc,
        message_class=template_pb2,
        callback=callback,
        host="localhost",
        port=50051,
    )
    server.start()
"""


class GRPCServer:
    def __init__(
        self, service_module, message_class, callback, host="localhost", port=50051
    ):
        class Servicer(service_module.ServiceServicer):
            def REQUEST(self, request, context):
                return callback(request)

        self.service_module = service_module
        self.service_class = Servicer()
        self.message_class = message_class
        self.callback = callback
        self.host = host
        self.port = port

    def start(self):
        try:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            self.service_module.add_ServiceServicer_to_server(
                self.service_class, server
            )
            server.add_insecure_port(f"{self.host}:{self.port}")
            server.start()
            print(f"gRPC Server started on port {self.port}")
            server.wait_for_termination()
        except KeyboardInterrupt:
            print("gRPC Server stopped by user.")
        except Exception as e:
            print(f"Error starting gRPC server: {e}")


"""
Example gRPC Client Usage:

import lemegeton.protocol.protobuf.template.template_pb2 as template_pb2
import lemegeton.protocol.protobuf.template.template_pb2_grpc as template_pb2_grpc
from lemegeton.protocol.protobuf.grpc_protocol import GRPCClient


def run():
    client = GRPCClient(
        stub_class=template_pb2_grpc.ServiceStub,
        proto_module=template_pb2,
    )
    response = client.request(name="Alice", age=30)
    print("Server 回傳：", response)


if __name__ == "__main__":
    run()


"""


class GRPCClient:
    def __init__(self, stub_class, proto_module, host="localhost", port=50051):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = stub_class(self.channel)
        self.proto_module = proto_module

    def request(self, name, age):
        msg = self.proto_module.MESSAGE(name=name, age=age)
        response = self.stub.REQUEST(msg)
        return response.message
