import threading
import time
from enum import Enum, auto
from typing import Optional

import zmq

from lemegeton.gateway import Gateway, GatewayStatus, ServiceType, query_service_info
from lemegeton.serializer import ProtobufMessageHandler


class ClientStatue(Enum):
    Connected = auto()
    Disconnected = auto()
    Querying = auto()
    Standby = auto()


class ClientCore:
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        expect_service_type: ServiceType,
        ip_address: str,
        query_port: int,
    ):
        self._context = context
        self._name = name
        self._expect_service_type = expect_service_type
        self._ip_address = ip_address
        self._query_port = query_port

        self._query_envet = threading.Event()
        self._error_event = threading.Event()
        self._connect_event = threading.Event()
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread = None

    def _connect(self):
        self._connect_event.set()
        if self._heartbeat_thread:
            self._heartbeat_stop_event.set()
            self._heartbeat_thread.join()
            self._heartbeat_stop_event.clear()

        self._query_envet.set()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat(self):
        while not self._heartbeat_stop_event.is_set():
            self._endpoint, self._query_endpoint_message = self._query_gateway()
            if not self._endpoint:
                self._error_event.set()
            else:
                self._error_event.clear()
            self._query_envet.clear()
            time.sleep(0.5)

    @property
    def status(self):
        if self._query_envet.is_set():
            return ClientStatue.Querying
        elif self._error_event.is_set():
            return ClientStatue.Disconnected
        elif self._connect_event.is_set():
            return ClientStatue.Standby
        else:
            return ClientStatue.Connected

    def _query_gateway(self):
        def _get_endpoint(service_ip, service_info):
            if service_ip == "localhost":
                if service_info["endpoint"]["ipc"] is not None:
                    return f"ipc://{service_info['endpoint']['ipc']}"
                else:
                    return f"tcp://{service_ip}:{service_info['endpoint']['tcp']}"
            else:
                if service_info["endpoint"]["tcp"] is not None:
                    return f"tcp://{service_ip}:{service_info['endpoint']['tcp']}"
                else:
                    return None

        info = query_service_info(
            self._context,
            self._name,
            ip_address=self._ip_address,
            port=self._query_port,
            timeout=2000,
        )

        if info["status"] != GatewayStatus.FOUND.value:
            return (
                None,
                "Service not found or inaccessible! Please ensure the service is running and the name is correct.",
            )

        info_data = info.get("data")

        if info_data["type"] != self._expect_service_type.value:
            return (
                None,
                f"Service type mismatch! The service is {info_data['type']}, but expected {self._expect_service_type.value}.",
            )

        endpoint = _get_endpoint(self._ip_address, info_data)
        if endpoint is None:
            return (
                None,
                "Service is not accessible for external client.",
            )
        else:
            return (
                endpoint,
                "Service query success.",
            )

    def close(self):
        self._heartbeat_stop_event.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join()


class Requester(ClientCore):
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        message_class,
        response_class,
        ip_address: str = "localhost",
        query_port: Optional[int] = Gateway.DEFAULT_QUERY_PORT,
        timeout: float = 3.0,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.RESPONDER,
            ip_address=ip_address,
            query_port=query_port,
        )

        self._message_class = message_class
        self._response_class = response_class
        self._timeout_ms = int(timeout * 1000.0)

        self._socket = None
        self._last_endpoint = None
        self._connect()
        if self.status == ClientStatue.Standby:
            self._init_socket()

    def _init_socket(self):
        """初始化或重置 Socket 狀態"""
        if self._socket:
            self._socket.close(linger=0)

        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.REQ_RELAXED, 1)
        self._socket.setsockopt(zmq.REQ_CORRELATE, 1)

        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)
        self._last_endpoint = self._endpoint
        self._connect_event.clear()

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(f"Warning: Expected {self._message_class.__name__}")
            return None

        if self.status == ClientStatue.Querying:
            print("Waiting for gateway responding......")
            return None

        elif self.status == ClientStatue.Disconnected:
            print(f"[{self._name}] {self._query_endpoint_message}")
            self._connect()
            return None

        elif self._endpoint != self._last_endpoint:
            print(f"[{self._name}] endpoint has changed.")
            self._init_socket()

        elif self.status == ClientStatue.Standby:
            self._init_socket()

        try:
            message_bytes = ProtobufMessageHandler.serialize(message)
            self._socket.send(message_bytes)

            response_bytes = self._socket.recv()
            return ProtobufMessageHandler.deserialize(
                self._response_class, response_bytes
            )
        except zmq.Again:
            return None

        except zmq.ContextTerminated:
            self.close()

        except Exception as e:
            print(f"[{self._name}] Request error: {e}")
            self.close()
            raise Exception(e)

    def is_connect(self):
        return self.status == ClientStatue.Connected

    def close(self):
        if self._socket:
            self._socket.close(linger=0)
        super().close()


class Publisher(ClientCore):
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        message_class,
        ip_address: str = "localhost",
        query_port: Optional[int] = Gateway.DEFAULT_QUERY_PORT,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.SUBSCRIBER,
            ip_address=ip_address,
            query_port=query_port,
        )

        self._message_class = message_class

        self._socket = None
        self._last_endpoint = None
        self._connect()
        if self.status == ClientStatue.Standby:
            self._init_socket()
        # else:
        #     print(f"[{self._name}] {self._query_endpoint_message}")

    def _init_socket(self):
        """初始化或重置 Socket 狀態"""
        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)
        self._last_endpoint = self._endpoint
        self._connect_event.clear()

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"[{self._name}] Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return

        if self.status == ClientStatue.Querying:
            print("Waiting for gateway responding......")
            return

        elif self.status == ClientStatue.Disconnected:
            print(f"[{self._name}] {self._query_endpoint_message}")
            self._connect()
            return

        elif self._endpoint != self._last_endpoint:
            print(f"[{self._name}] endpoint has changed.")
            self._init_socket()

        elif self.status == ClientStatue.Standby:
            self._init_socket()

        message_bytes = ProtobufMessageHandler.serialize(message)
        self._socket.send(message_bytes)

    def is_connect(self):
        return self.status == ClientStatue.Connected

    def close(self):
        self._socket.close()
        super().close()


class Subscriber(ClientCore):
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        message_class,
        callback,
        ip_address: str = "localhost",
        query_port: Optional[int] = Gateway.DEFAULT_QUERY_PORT,
        timeout: float = 60.0,
        buffer_size: int = 100,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.PUBLISHER,
            ip_address=ip_address,
            query_port=query_port,
        )
        self._message_class = message_class
        self._callback = callback
        self._timeout = int(timeout * 1000)  # 轉換為毫秒
        self._buffer_size = buffer_size
        self._sub_stop_event = threading.Event()
        self._sub_thread = None

        self._socket = None
        self._last_endpoint = None
        self._connect()
        if self.status == ClientStatue.Standby:
            self._init_socket()
        # else:
        #     print(f"[{self._name}] {self._query_endpoint_message}")

        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _init_socket(self):
        """專門負責 Socket 的初始化與訂閱設定"""
        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout)
        self._socket.setsockopt(zmq.RCVHWM, self._buffer_size)
        self._socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
        self._socket.connect(self._endpoint)
        self._last_endpoint = self._endpoint
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._connect_event.clear()

    def _subscribe_process(self):
        while not self._sub_stop_event.is_set():
            try:
                if self.status == ClientStatue.Querying:
                    print("Waiting for gateway responding......")
                    time.sleep(0.1)
                    continue

                elif self.status == ClientStatue.Disconnected:
                    self._connect()
                    time.sleep(0.1)
                    continue

                elif self._endpoint != self._last_endpoint:
                    print(f"[{self._name}] endpoint has changed.")
                    self._init_socket()

                elif self.status == ClientStatue.Standby:
                    self._init_socket()

                # 2. 接收與反序列化
                message_bytes = self._socket.recv()
                message = ProtobufMessageHandler.deserialize(
                    self._message_class, message_bytes
                )
            except zmq.Again:
                continue
            except zmq.ContextTerminated:
                break
            except Exception as e:
                print(f"[{self._name}] Process Error: {e}")
                if self._sub_stop_event.is_set():
                    break
                continue

            try:
                self._callback(message)
            except Exception as e:
                print(f"[{self._name}] Callback execution error: {e}")

        # 離開迴圈後清理
        if self._socket:
            self._socket.close(linger=0)
        super().close()

    def is_connect(self):
        return self.status == ClientStatue.Connected

    def close(self):
        self._sub_stop_event.set()
        if self._sub_thread:
            self._sub_thread.join(timeout=2.0)


class ActionClient(ClientCore):
    pass
