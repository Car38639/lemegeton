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


def _worker_routine(
    worker_id, backend_ipc_path, message_class, response_class, callback, stop_event
):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect(f"ipc://{backend_ipc_path}")

    while not stop_event.is_set():
        try:
            if not socket.poll(500):
                continue
            message_bytes = socket.recv()

            try:
                message = ProtobufMessageHandler.deserialize(
                    message_class, message_bytes
                )
            except Exception:
                print(f"Wrong message type, expect:{message_class.__name__}")
                empty_response = response_class()
                socket.send(ProtobufMessageHandler.serialize(empty_response))
                continue

            # Handle the request
            response_message = callback(message)
            if not isinstance(response_message, response_class):
                print(
                    f"Warning: Wrong message class, expect: {response_class.__name__}"
                )
                response_message = response_class()

            response_bytes = ProtobufMessageHandler.serialize(response_message)
            socket.send(response_bytes)
        except KeyboardInterrupt:
            break
        except zmq.ContextTerminated:
            break
        except Exception as e:
            print(f"[Worker-{worker_id}] Unexpected Error: {e}")
            break
    socket.close(linger=0)
    context.term()
    print(f"Worker {worker_id} cleaned up and exited.")


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
        worker_num: int = 1,
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
        self._worker_num = worker_num

        self._frontend_sock = self._context.socket(zmq.ROUTER)
        self._frontend_sock.setsockopt(zmq.LINGER, 0)
        self._frontend_sock.setsockopt(zmq.RCVHWM, buffer_size)
        if self._enable_tcp:
            try:
                self._frontend_sock.bind(f"tcp://*:{self._tcp_port}")
            except Exception as e:
                print(f"[{name}] Fail to bind {self._tcp_port}: {e}")
                raise Exception(f"[{name}] Fail to bind {self._tcp_port}: {e}")

        if self._enable_ipc:
            self._frontend_sock.bind(f"ipc://{self._ipc_path}")

        self._backend_ipc_path = f"/tmp/{self._name}_internal_backend.ipc"
        self._backend_sock = self._context.socket(zmq.DEALER)
        self._backend_sock.bind(f"ipc://{self._backend_ipc_path}")
        self._workers = []

        self._worker_stop_event = multiprocessing.Event()
        self._routing_thread = threading.Thread(
            target=self._routing_process, daemon=True
        )
        self._routing_thread.start()

    def _routing_process(self):
        print("[Broker] 啟動中...")
        if self._enable_tcp:
            print(f" - 前端 TCP: tcp://*:{self._tcp_port}")
        if self._enable_ipc:
            print(f" - 前端 IPC: ipc://{self._ipc_path}")
        print(
            f" - 後端分發: {self._backend_ipc_path} (Worker 數量: {self._worker_num})"
        )

        # 啟動 Workers
        for i in range(self._worker_num):
            p = multiprocessing.Process(
                target=_worker_routine,
                args=(
                    i,
                    self._backend_ipc_path,
                    self._message_class,
                    self._response_class,
                    self._callback,
                    self._worker_stop_event,
                ),
            )
            p.daemon = True
            p.start()
            self._workers.append(p)

        try:
            zmq.proxy(self._frontend_sock, self._backend_sock)

        except (zmq.ZMQError, zmq.ContextTerminated):
            print(f"[{self._name}] Responder 已成功停止")
        finally:
            if not self._worker_stop_event.is_set():
                self._worker_stop_event.set()
                self._cleanup()

    def _cleanup(self):
        self._heartbeat_client.stop()
        self._frontend_sock.close(linger=0)
        self._backend_sock.close(linger=0)
        for p in self._workers:
            p.join(timeout=5.0)
            if p.is_alive():
                print(f"Worker {p.pid} did not exit in time, terminating...")
                p.terminate()

    def close(self):
        self._worker_stop_event.set()
        self._cleanup()
        if self._routing_thread:
            self._routing_thread.join()


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

    def publish(self, message):
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
