import threading
from typing import Optional

import zmq
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct


class ProtobufMessageHandler:
    """
    ProtobufMessageHandler handles serialization and deserialization of protobuf messages.
    """

    @staticmethod
    def serialize(message) -> bytes:
        return message.SerializeToString()

    @staticmethod
    def deserialize(message_class, message_bytes: bytes):
        message = message_class()
        message.ParseFromString(message_bytes)
        return message


class Publisher:
    """
    Publisher manages ZeroMQ communication for publishing protobuf messages.
    """

    def __init__(
        self,
        message_class,
        context: Optional[zmq.Context] = None,
        ip_address: Optional[str] = None,
        port: int = 60001,
    ):
        self._message_class = message_class
        self._port = port
        self._ip_address = ip_address

        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False

        self._context = context

        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.SNDHWM, 1)
        if self._ip_address:
            self._socket.connect(f"tcp://{self._ip_address}:{self._port}")
        else:
            self._socket.bind(f"tcp://*:{self._port}")

    def publish(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return
        message_bytes = ProtobufMessageHandler.serialize(message)
        self._socket.send(message_bytes)

    def close(self):
        self._socket.close()
        if self._use_temp_context:
            self._context.term()


class Subscriber:
    """
    Subscriber manages ZeroMQ communication for subscribing to protobuf messages.
    """

    def __init__(
        self,
        message_class,
        callback,
        context: Optional[zmq.Context] = None,
        ip_address: Optional[str] = None,
        port: int = 60001,
    ):
        self._message_class = message_class
        self._callback = callback

        self._ip_address = ip_address
        self._port = port
        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False
        self._context = context
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVHWM, 1)
        if self._ip_address:
            self._socket.connect(f"tcp://{self._ip_address}:{self._port}")
        else:
            self._socket.bind(f"tcp://*:{self._port}")
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")

        self._sub_stop_event = threading.Event()
        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _subscribe_process(self):
        while not self._sub_stop_event.is_set():
            try:
                message_bytes = self._socket.recv(flags=zmq.NOBLOCK)
                try:
                    message = ProtobufMessageHandler.deserialize(
                        self._message_class, message_bytes
                    )
                except Exception:
                    print(f"Wrong message type, expect:{self._message_class.__name__}")
                    continue
            except zmq.Again:
                continue

            self._callback(message)

    def close(self):
        self._sub_stop_event.set()
        self._sub_thread.join()
        self._socket.close()
        if self._use_temp_context:
            self._context.term()


class Requester:
    """
    Requester manages ZeroMQ communication for protobuf messages.
    """

    def __init__(
        self,
        message_class,
        response_class,
        context: Optional[zmq.Context] = None,
        ip_address: Optional[str] = None,
        port: int = 60001,
    ):
        self._message_class = message_class
        self._response_class = response_class
        self._ip_address = ip_address
        self._port = port
        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False
        self._context = context
        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.RCVHWM, 1)
        self._socket.setsockopt(zmq.LINGER, 0)
        if self._ip_address:
            self._socket.connect(f"tcp://{self._ip_address}:{self._port}")
        else:
            self._socket.bind(f"tcp://*:{self._port}")

    def request(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return None

        message_bytes = ProtobufMessageHandler.serialize(message)
        self._socket.send(message_bytes)

        response_bytes = self._socket.recv()
        response_message = ProtobufMessageHandler.deserialize(
            self._response_class, response_bytes
        )
        return response_message

    def close(self):
        self._socket.close()
        if self._use_temp_context:
            self._context.term()


class Responder:
    """
    Responder manages ZeroMQ communication for protobuf messages.
    """

    def __init__(
        self,
        message_class,
        response_class,
        callback,
        context: Optional[zmq.Context] = None,
        ip_address: Optional[str] = None,
        port: int = 60001,
    ):
        self._message_class = message_class
        self._response_class = response_class
        self._callback = callback
        self._ip_address = ip_address
        self._port = port

        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False

        self._context = context

        self._socket = self._context.socket(zmq.REP)
        self._socket.setsockopt(zmq.SNDHWM, 1)
        if self._ip_address:
            self._socket.connect(f"tcp://{self._ip_address}:{self._port}")
        else:
            self._socket.bind(f"tcp://*:{self._port}")

        self._server_stop_event = threading.Event()
        self._server_thread = threading.Thread(target=self._serve, daemon=True)
        self._server_thread.start()

    def _serve(self):
        while not self._server_stop_event.is_set():
            try:
                message_bytes = self._socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                continue

            try:
                message = ProtobufMessageHandler.deserialize(
                    self._message_class, message_bytes
                )
            except Exception:
                print(f"Wrong message type, expect:{self._message_class.__name__}")
                empty_response = self._response_class()
                self._socket.send(ProtobufMessageHandler.serialize(empty_response))
                continue

            # Handle the request
            response_message = self._callback(message)
            if not isinstance(response_message, self._response_class):
                print(
                    f"Warning: Wrong message class, expect: {self._response_class.__name__}"
                )
                response_message = self._response_class()

            response_bytes = ProtobufMessageHandler.serialize(response_message)
            self._socket.send(response_bytes)

    def close(self):
        self._server_stop_event.set()
        self._server_thread.join()
        self._socket.close()
        if self._use_temp_context:
            self._context.term()


class Pusher:
    """
    Pusher manages ZeroMQ PUSH communication for protobuf messages.
    """

    def __init__(
        self,
        message_class,
        context: Optional[zmq.Context] = None,
        ip_address: Optional[str] = None,
        port: int = 60001,
    ):
        self._message_class = message_class
        self._ip_address = ip_address
        self._port = port

        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False

        self._context = context
        self._socket = self._context.socket(zmq.PUSH)
        self._socket.setsockopt(zmq.SNDHWM, 1)
        if self._ip_address:
            self._socket.connect(f"tcp://{self._ip_address}:{self._port}")
        else:
            self._socket.bind(f"tcp://*:{self._port}")

    def push(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return

        message_bytes = ProtobufMessageHandler.serialize(message)
        try:
            self._socket.send(message_bytes, flags=zmq.NOBLOCK)

        except zmq.Again:
            pass

    def close(self):
        self._socket.close()
        if self._use_temp_context:
            self._context.term()


class Puller:
    """
    Puller manages ZeroMQ PULL communication for protobuf messages.
    """

    def __init__(
        self,
        message_class,
        callback,
        context: Optional[zmq.Context] = None,
        ip_address: Optional[str] = None,
        port: int = 60001,
    ):
        self._message_class = message_class
        self._callback = callback

        self._ip_address = ip_address
        self._port = port
        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False
        self._context = context
        self._socket = self._context.socket(zmq.PULL)
        self._socket.setsockopt(zmq.RCVHWM, 1)
        if self._ip_address:
            self._socket.connect(f"tcp://{self._ip_address}:{self._port}")
        else:
            self._socket.bind(f"tcp://*:{self._port}")

        self._pull_stop_event = threading.Event()
        self._pull_thread = threading.Thread(target=self._pull_process, daemon=True)
        self._pull_thread.start()

    def _pull_process(self):
        while not self._pull_stop_event.is_set():
            try:
                message_bytes = self._socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                continue
            try:
                message = ProtobufMessageHandler.deserialize(
                    self._message_class, message_bytes
                )
            except Exception:
                print(f"Wrong message type, expect:{self._message_class.__name__}")
                continue

            self._message = message
            self._callback(message)

    def close(self):
        self._pull_stop_event.set()
        self._pull_thread.join()
        self._socket.close()
        if self._use_temp_context:
            self._context.term()


class Router:
    """
    Router manages ZeroMQ ROUTER communication for protobuf messages.
    """

    def __init__(
        self,
        context: Optional[zmq.Context] = None,
        port: int = 60001,
    ):
        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False

        self._context = context
        self._port = port

        self._worker_dict = {}

        self._entry_socket = self._context.socket(zmq.ROUTER)
        self._entry_socket.bind(f"tcp://*:{self._port}")

        self._result_receiver = self._context.socket(zmq.PULL)
        self._result_bus_addr = f"inproc://{self._port}_result_bus"
        self._result_receiver.bind(self._result_bus_addr)

        self._task_poller = zmq.Poller()
        self._task_poller.register(self._entry_socket, zmq.POLLIN)
        self._task_poller.register(self._result_receiver, zmq.POLLIN)

        self._router_stop_event = threading.Event()
        self._routing_thread = threading.Thread(target=self._start_routing, daemon=True)
        self._routing_thread.start()

        def _handle_info_request(message):
            worker_info = {
                name: {
                    "description": info["description"],
                    "message_class": info["message_class"].__name__,
                    "response_class": info["response_class"].__name__
                    if info["response_class"]
                    else None,
                }
                for name, info in self._worker_dict.items()
            }
            response_struct = ParseDict(worker_info, Struct())
            return response_struct

        self.register_worker(
            name="info",
            description="Provides information about registered workers in this router",
            message_class=Struct,
            callback=_handle_info_request,
            response_class=Struct,
        )

    def _start_routing(self):
        while not self._router_stop_event.is_set():
            socks = dict(self._task_poller.poll(100))
            if self._entry_socket in socks:
                identity, name, message = self._entry_socket.recv_multipart()
                task_thread = threading.Thread(
                    target=self._handle_task,
                    args=(name.decode(), message, identity),
                    daemon=True,
                )
                task_thread.start()

            if self._result_receiver in socks:
                identity, response_bytes = self._result_receiver.recv_multipart()
                self._entry_socket.send_multipart([identity, response_bytes])

    def _handle_task(self, name, message, identity):
        if name not in self._worker_dict:
            raise ValueError(f"No worker registered with name '{name}'")
        callback = self._worker_dict[name]["callback"]
        message_class = self._worker_dict[name]["message_class"]

        try:
            message = ProtobufMessageHandler.deserialize(message_class, message)
        except Exception:
            print(f"Wrong message type, expect:{message_class.__name__}")
            if self._worker_dict[name]["response_class"]:
                empty_response = self._worker_dict[name]["response_class"]()
                response_bytes = ProtobufMessageHandler.serialize(empty_response)
                internal_sender = self._context.socket(zmq.PUSH)
                internal_sender.connect(self._result_bus_addr)
                internal_sender.send_multipart([identity, response_bytes])
                internal_sender.close()  # 傳完即焚
                return

        response_message = callback(message)
        if self._worker_dict[name]["response_class"] is None:
            response_bytes = b""

        elif self._worker_dict[name]["response_class"] and not isinstance(
            response_message, self._worker_dict[name]["response_class"]
        ):
            raise Exception(
                f"Warning: Wrong message class, except: {self._worker_dict[name]['response_class'].__name__}"
            )

        else:
            response_bytes = ProtobufMessageHandler.serialize(response_message)

        internal_sender = self._context.socket(zmq.PUSH)
        internal_sender.connect(self._result_bus_addr)
        internal_sender.send_multipart([identity, response_bytes])
        internal_sender.close()  # 傳完即焚

    def register_worker(
        self,
        name,
        description,
        message_class,
        callback,
        response_class=None,
    ):
        if name in self._worker_dict:
            raise ValueError(f"Worker with name '{name}' already exists.")

        self._worker_dict[name] = {
            "description": description,
            "message_class": message_class,
            "callback": callback,
            "response_class": response_class,
        }

    def close(self):
        self._router_stop_event.set()
        self._routing_thread.join()
        self._entry_socket.close()
        self._result_receiver.close()
        if self._use_temp_context:
            self._context.term()


def request_router_info(ip_address: str = "localhost", port: int = 60001):
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(f"tcp://{ip_address}:{port}")
    socket.send_multipart([b"info", b""])
    info_bytes = socket.recv_multipart()
    msg = Struct()
    msg.ParseFromString(info_bytes[0])
    info = MessageToDict(msg)
    socket.close()
    context.term()
    return info


class Dealer:
    """
    Dealer manages ZeroMQ DEALER communication for protobuf messages.
    """

    def __init__(
        self,
        message_class,
        response_class=None,
        context: Optional[zmq.Context] = None,
        ip_address: str = "localhost",
        port: int = 60001,
        task_name: str = "",
    ):
        if context is None:
            context = zmq.Context()
            self._use_temp_context = True
        else:
            self._use_temp_context = False

        self._message_class = message_class
        self._response_class = response_class

        self._context = context
        self._ip_address = ip_address
        self._port = port
        self._task_name = task_name.encode("utf-8")

        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(f"tcp://{self._ip_address}:{self._port}")

        self._poller = zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return

        message_bytes = ProtobufMessageHandler.serialize(message)
        try:
            self._socket.send_multipart(
                [self._task_name, message_bytes], flags=zmq.NOBLOCK
            )

        except zmq.Again:
            pass

        while True:
            socks = dict(self._poller.poll(100))
            if self._socket in socks:
                response_list = self._socket.recv_multipart()
                response_bytes = response_list[0]
                response_message = None
                if self._response_class is not None:
                    response_message = ProtobufMessageHandler.deserialize(
                        self._response_class, response_bytes
                    )
                break

        return response_message

    def close(self):
        self._socket.close()
        if self._use_temp_context:
            self._context.term()
