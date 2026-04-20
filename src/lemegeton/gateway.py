import importlib
import socket
from typing import Optional

import zmq
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct

from lemegeton.core import Publisher, Puller, Pusher, Requester, Responder, Subscriber

GATEWAY_PORT = 60000


def _get_message_import_path(message_class):
    return f"{message_class.__module__}.{message_class.__name__}"


def _get_message_class(import_path):
    module_path, class_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _allocate_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class Gateway:
    def __init__(self, port: int = GATEWAY_PORT):
        self._context = zmq.Context()
        self._query_responder = Responder(
            response_class=Struct,
            message_class=Struct,
            callback=self._handle_query,
            context=self._context,
            port=port,
        )
        self._protocol_list = {}

    def _handle_query(self, request: Struct) -> Struct:
        req = MessageToDict(request)

        info = self._protocol_list.get(req.get("name"), {"info": {"port": None}})[
            "info"
        ]
        response_struct = ParseDict(info, Struct())
        return response_struct

    def _query_protocol_info(
        self,
        name: str,
        ip_address: str,
        port: int,
        timeout: int = 2000,
    ) -> dict:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, timeout)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(f"tcp://{ip_address}:{port}")
        request_struct = Struct()
        request_struct["name"] = name
        try:
            socket.send(request_struct.SerializeToString())
            response_bytes = socket.recv()
            response_struct = Struct()
            response_struct.ParseFromString(response_bytes)
            info = MessageToDict(response_struct)
            if info.get("port") is not None:
                protocol_type = info.get("type")
                port = int(info.get("port"))
                ret = (protocol_type, port)

            else:
                print(f"Protocol '{name}' not found in gateway {ip_address}:{port}.")
                ret = (None, None)
        except zmq.Again:
            print(
                f"Failed to query protocol '{name}' from gateway at {ip_address}:{port}."
            )
            ret = (None, None)
        finally:
            socket.close()
            context.term()
            return ret

    def register_publisher(self, name, message_class, ip_address: Optional[str] = None):
        if name in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is already registered.")
        port = _allocate_port()
        protocol = Publisher(
            message_class=message_class,
            context=self._context,
            ip_address=ip_address,
            port=port,
        )
        self._protocol_list[name] = {
            "protocol": protocol,
            "info": {
                "type": "publisher",
                "port": port,
            },
        }

    def register_responder(
        self,
        name,
        message_class,
        response_class,
        callback,
        ip_address: Optional[str] = None,
    ):
        if name in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is already registered.")
        port = _allocate_port()
        protocol = Responder(
            response_class=response_class,
            message_class=message_class,
            callback=callback,
            context=self._context,
            ip_address=ip_address,
            port=port,
        )
        self._protocol_list[name] = {
            "protocol": protocol,
            "info": {
                "type": "responder",
                "port": port,
                "binding": ip_address if ip_address else "*",
            },
        }

    def register_pusher(
        self,
        name,
        message_class,
        ip_address: Optional[str] = None,
    ):
        if name in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is already registered.")
        port = _allocate_port()
        protocol = Pusher(
            message_class=message_class,
            context=self._context,
            ip_address=ip_address,
            port=port,
        )
        self._protocol_list[name] = {
            "protocol": protocol,
            "info": {
                "type": "pusher",
                "port": port,
                "binding": ip_address if ip_address else "*",
            },
        }

    def _get_protocol_setting(
        self,
        name: str,
        corresponding_protocol_type: str,
        ip_address: Optional[str] = None,
        port: Optional[int] = None,
    ):
        """
        server 模式： binding *:(auto allocated_port)
        client 模式： connect IP:port, 資訊從 gateway 取得
        """
        if ip_address is not None and port is None:
            raise ValueError("Port must be provided when IP address is specified.")
        elif ip_address is None and port is not None:
            raise ValueError("IP address must be provided when port is specified.")

        elif ip_address is not None and port is not None:
            protocol_type, port = self._query_protocol_info(
                name, ip_address=ip_address, port=port
            )

            if protocol_type is None:
                raise ValueError(
                    f"Protocol '{name}' not found in gateway at {ip_address}:{port}."
                )

            if protocol_type != corresponding_protocol_type:
                raise ValueError(
                    f"Protocol '{name}' is {protocol_type}, not {corresponding_protocol_type}."
                )
            return ip_address, port

        elif ip_address is None and port is None:
            return None, _allocate_port()

    def register_subscriber(
        self,
        name,
        message_class,
        callback,
        port: Optional[int] = None,
        ip_address: Optional[str] = None,
    ):
        ip_address, port = self._get_protocol_setting(
            name,
            corresponding_protocol_type="publisher",
            ip_address=ip_address,
            port=port,
        )

        protocol = Subscriber(
            message_class=message_class,
            ip_address=ip_address,
            port=port,
            callback=callback,
        )
        # self._protocol_list[name] = {
        #     "protocol": protocol,
        #     "info": {
        #         "type": "subscriber",
        #         "port": port,
        #         "binding": ip_address if ip_address else "*",
        #     },
        # }

    def register_puller(
        self,
        name,
        message_class,
        callback,
        port: Optional[int] = None,
        ip_address: Optional[str] = None,
    ):
        ip_address, port = self._get_protocol_setting(
            name,
            corresponding_protocol_type="pusher",
            ip_address=ip_address,
            port=port,
        )

        protocol = Puller(
            message_class=message_class,
            ip_address=ip_address,
            port=port,
            callback=callback,
        )
        # self._protocol_list[name] = {
        #     "protocol": protocol,
        #     "info": {
        #         "type": "puller",
        #         "port": port,
        #         "binding": ip_address if ip_address else "*",
        #     },
        # }

    def register_requester(
        self,
        name,
        message_class,
        response_class,
        ip_address: Optional[str] = None,
        port: Optional[int] = None,
    ):
        ip_address, port = self._get_protocol_setting(
            name,
            corresponding_protocol_type="responder",
            ip_address=ip_address,
            port=port,
        )

        protocol = Requester(
            response_class=response_class,
            message_class=message_class,
            ip_address=ip_address,
            port=port,
        )
        # self._protocol_list[name] = {
        #     "protocol": protocol,
        #     "info": {
        #         "type": "requester",
        #         "port": port,
        #         "binding": ip_address if ip_address else "*",
        #     },
        # }

    def send(self, name, message):
        if name not in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is not registered.")
        protocol_info = self._protocol_list[name]
        protocol = protocol_info["protocol"]
        if isinstance(protocol, Publisher):
            protocol.publish(message)
        elif isinstance(protocol, Responder):
            raise ValueError(f"Protocol '{name}' is a responder. Use 'request' method.")
        elif isinstance(protocol, Pusher):
            protocol.push(message)
        else:
            raise ValueError(f"Unknown protocol type for '{name}'.")

    def remove(self, name):
        if name not in self._protocol_list:
            print(f"Protocol '{name}' is not registered.")
            return
        protocol_info = self._protocol_list.pop(name)
        protocol_info["protocol"].close()

    def close(self):
        self._query_responder.close()
        for protocol_info in self._protocol_list.values():
            protocol_info["protocol"].close()
        self._context.term()
