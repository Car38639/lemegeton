import multiprocessing
import socket
import threading
from typing import Literal, Optional

import zmq

from lemegeton.gateway import HeartbeatClient, ServiceType
from lemegeton.serializer import ProtobufMessageHandler


class ServiceCore:
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        service_type: ServiceType,
        port: Optional[int] = None,
        mode: Literal["tcp", "ipc", "both"] = "tcp",
    ):
        self._enable_ipc, self._enable_tcp = False, False

        self._name = name
        self._tcp_port = port if port is not None else self._allocate_port()

        if mode == "tcp" or mode == "both":
            self._enable_tcp = True

        if mode == "ipc" or mode == "both":
            self._enable_ipc = True
            self._ipc_path = f"@{self._name}_{self._tcp_port}"

        try:
            self._heartbeat_client = HeartbeatClient(
                context=context,
                name=self._name,
                data={
                    "endpoint": {
                        "tcp": self._tcp_port if self._enable_tcp else None,
                        "ipc": self._ipc_path if self._enable_ipc else None,
                    },
                    "type": service_type.value,
                    "mode": mode,
                },
            )
        except Exception as e:
            raise Exception(
                f"[{self._name}] Heartbeat client initialization failed: {e}"
            )

    def _allocate_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]




class Responder(ServiceCore):
    def __init__(
        self,
        context,
        name,
        message_class,
        response_class,
        callback,
        mode: Literal["tcp", "ipc", "both"] = "tcp",
        port: Optional[int] = None,
        buffer_size: int = 100,
    ):
        try:
            super().__init__(context, name, ServiceType.RESPONDER, port, mode)
        except Exception as e:
            raise Exception(f"[{name}] Core initialization failed: {e}")
        self._context = context
        self._message_class = message_class
        self._response_class = response_class
        self._callback = callback
        self._mode = mode

        self._socket = self._context.socket(zmq.REP)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.RCVHWM, buffer_size)
        if self._enable_tcp:
            try:
                self._socket.bind(f"tcp://*:{self._tcp_port}")
            except Exception as e:
                print(f"[{name}] Fail to bind {self._tcp_port}: {e}")
                raise Exception(f"[{name}] Fail to bind {self._tcp_port}: {e}")

        if self._enable_ipc:
            self._socket.bind(f"ipc://{self._ipc_path}")

        self._stop_event = threading.Event()
        self._response_thread = threading.Thread(
            target=self._response_process, daemon=True
        )
        self._response_thread.start()

    def _response_process(self):
        while not self._stop_event.is_set():
            try:
                if not self._socket.poll(100):
                    continue

                message_bytes = self._socket.recv()
                try:
                    message = ProtobufMessageHandler.deserialize(
                        self._message_class, message_bytes
                    )
                except Exception:
                    print(f"Wrong message type, expect:{self._message_class.__name__}")
                    empty_response = self._message_class()
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

            except Exception as e:
                print(f"[{self._name}] Callback execution error: {e}")

            except zmq.ContextTerminated:
                # 當 context 被關閉時，優雅退出
                print(f"[{self._name}] Context terminated.")
                break
            except Exception as e:
                # 捕捉其他非預期的 ZMQ 錯誤
                print(f"[{self._name}] Unexpected error: {e}")
                if self._stop_event.is_set():
                    break

        self._heartbeat_client.stop()
        if self._socket:
            self._socket.close(linger=0)


    def close(self):
        self._stop_event.set()
        if self._response_thread:
            self._response_thread.join()


class Publisher(ServiceCore):
    """
    Publisher manages ZeroMQ communication for publishing protobuf messages.
    """

    def __init__(
        self,
        context: zmq.Context,
        name: str,
        message_class,
        mode: Literal["tcp", "ipc", "both"] = "tcp",
        port: Optional[int] = None,
    ):
        try:
            super().__init__(context, name, ServiceType.PUBLISHER, port, mode)
        except Exception as e:
            raise Exception(f"[{name}] Core initialization failed: {e}")

        self._message_class = message_class

        self._mode = mode
        self._socket = context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.SNDHWM, 1)
        self._socket.setsockopt(zmq.LINGER, 0)
        if self._enable_tcp:
            try:
                self._socket.bind(f"tcp://*:{self._tcp_port}")
            except Exception as e:
                print(f"[{name}] Fail to bind {self._tcp_port}: {e}")
                raise Exception(f"[{name}] Fail to bind {self._tcp_port}: {e}")

        if self._enable_ipc:
            self._socket.bind(f"ipc://{self._ipc_path}")

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"[{self._name}] Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return
        message_bytes = ProtobufMessageHandler.serialize(message)
        self._socket.send(message_bytes)

    def close(self):
        self._heartbeat_client.stop()
        if self._socket:
            self._socket.close(linger=0)


class Subscriber(ServiceCore):
    """
    Subscriber manages ZeroMQ communication for subscribing to protobuf messages.
    """

    def __init__(
        self,
        context: zmq.Context,
        name: str,
        message_class,
        callback,
        mode: Literal["tcp", "ipc", "both"] = "tcp",
        port: Optional[int] = None,
        buffer_size: int = 100,
    ):
        try:
            super().__init__(context, name, ServiceType.SUBSCRIBER, port, mode)
        except Exception as e:
            print(f"[{name}] Core initialization failed: {e}")
            raise Exception(f"[{name}] Heartbeat client initialization failed: {e}")

        self._message_class = message_class
        self._callback = callback

        self._socket = context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.RCVHWM, buffer_size)

        if self._enable_tcp:
            try:
                self._socket.bind(f"tcp://*:{self._tcp_port}")
            except Exception as e:
                print(f"[{self._name}] Fail to bind {self._tcp_port}: {e}")
                raise Exception(f"[{self._name}] Fail to bind {self._tcp_port}: {e}")

        if self._enable_ipc:
            self._socket.bind(f"ipc://{self._ipc_path}")

        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")

        self._sub_stop_event = threading.Event()
        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _subscribe_process(self):
        while not self._sub_stop_event.is_set():
            try:
                if not self._socket.poll(100):
                    continue

                message_bytes = self._socket.recv()
                try:
                    message = ProtobufMessageHandler.deserialize(
                        self._message_class, message_bytes
                    )
                except Exception:
                    print(
                        f"[{self._name}] Wrong message type, expect:{self._message_class.__name__}"
                    )
                    continue
                try:
                    self._callback(message)
                except Exception as e:
                    print(f"[{self._name}] Callback execution error: {e}")

            except zmq.ContextTerminated:
                # 當 context 被關閉時，優雅退出
                print(f"[{self._name}] ZMQ Context terminated.")
                break
            except Exception as e:
                # 捕捉其他非預期的 ZMQ 錯誤
                print(f"[{self._name}] Unexpected error: {e}")
                if self._sub_stop_event.is_set():
                    break

        self._heartbeat_client.stop()
        if self._socket:
            self._socket.close(linger=0)

    def close(self):
        self._sub_stop_event.set()
        self._sub_thread.join()


def Broker(ServiceCore):
    """
    Broker is used to centralize the management of micro services in one port.
    """
    raise NotImplementedError("Broker is not implemented yet.")


def ActionServer(ServiceCor):
    pass
