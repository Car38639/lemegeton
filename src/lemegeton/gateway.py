import importlib
import socket

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

    def register_publisher(self, name, message_class):
        if name in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is already registered.")
        port = _allocate_port()
        protocol = Publisher(
            message_class=message_class,
            context=self._context,
            port=port,
        )
        self._protocol_list[name] = {
            "protocol": protocol,
            "info": {
                "type": "publisher",
                "message_class": _get_message_import_path(message_class),
                "response_class": None,
                "port": port,
            },
        }

    def register_responder(self, name, message_class, response_class, callback):
        if name in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is already registered.")
        port = _allocate_port()
        protocol = Responder(
            response_class=response_class,
            message_class=message_class,
            callback=callback,
            context=self._context,
            port=port,
        )
        self._protocol_list[name] = {
            "protocol": protocol,
            "info": {
                "type": "responder",
                "message_class": _get_message_import_path(message_class),
                "response_class": _get_message_import_path(response_class)
                if response_class
                else None,
                "port": port,
            },
        }

    def register_pusher(self, name, message_class):
        if name in self._protocol_list:
            raise ValueError(f"Protocol '{name}' is already registered.")
        port = _allocate_port()
        protocol = Pusher(
            message_class=message_class,
            context=self._context,
            port=port,
        )
        self._protocol_list[name] = {
            "protocol": protocol,
            "info": {
                "type": "pusher",
                "message_class": _get_message_import_path(message_class),
                "response_class": None,
                "port": port,
            },
        }

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


def _query_protocol_info(
    name: str, ip_address: str, port: int, timeout: int = 2000
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
            message_class = _get_message_class(info.get("message_class"))
            response_class = (
                _get_message_class(info.get("response_class"))
                if info.get("response_class")
                else None
            )
            ret = (protocol_type, port, message_class, response_class)

        else:
            print(f"Protocol '{name}' not found in gateway {ip_address}:{port}.")
            ret = (None, None, None, None)
    except zmq.Again:
        print(f"Failed to query protocol '{name}' from gateway at {ip_address}:{port}.")
        ret = (None, None, None, None)
    finally:
        socket.close()
        context.term()
        return ret


def create_subscriber(name, ip_address, callback, port: int = GATEWAY_PORT):
    protocol_type, protocol_port, message_class, _ = _query_protocol_info(
        name, ip_address=ip_address, port=port
    )
    if protocol_type is None:
        raise ValueError(
            f"Protocol '{name}' not found in gateway at {ip_address}:{port}."
        )

    if protocol_type != "publisher":
        raise ValueError(f"Protocol '{name}' is {protocol_type}.")
    if callback is None:
        raise ValueError(
            f"Callback function must be provided for subscriber protocol '{name}'."
        )
    return Subscriber(
        message_class=message_class,
        ip_address=ip_address,
        port=protocol_port,
        callback=callback,
    )


def create_requester(name, ip_address, port: int = GATEWAY_PORT):
    protocol_type, protocol_port, message_class, response_class = _query_protocol_info(
        name, ip_address=ip_address, port=port
    )
    if protocol_type is None:
        raise ValueError(
            f"Protocol '{name}' not found in gateway at {ip_address}:{port}."
        )

    if protocol_type != "responder":
        raise ValueError(f"Protocol '{name}' is {protocol_type}.")
    return Requester(
        response_class=response_class,
        message_class=message_class,
        ip_address=ip_address,
        port=protocol_port,
    )


def create_puller(name, ip_address, callback, port: int = GATEWAY_PORT):
    protocol_type, protocol_port, message_class, _ = _query_protocol_info(
        name, ip_address=ip_address, port=port
    )
    if protocol_type is None:
        raise ValueError(
            f"Protocol '{name}' not found in gateway at {ip_address}:{port}."
        )

    if protocol_type != "pusher":
        raise ValueError(f"Protocol '{name}' is {protocol_type}.")
    if callback is None:
        raise ValueError(
            f"Callback function must be provided for subscriber protocol '{name}'."
        )

    return Puller(
        message_class=message_class,
        ip_address=ip_address,
        port=protocol_port,
        callback=callback,
    )
