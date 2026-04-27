import threading
import time
from typing import Optional

import zmq

from lemegeton.gateway import Gateway, GatewayStatus, ServiceType, query_service_info
from lemegeton.serializer import ProtobufMessageHandler


class ClientCore:
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        expect_service_type: ServiceType,
        ip_address: str,
        query_port: int,
    ):
        info = query_service_info(
            context, name, ip_address=ip_address, port=query_port, timeout=2000
        )

        if info["status"] != GatewayStatus.FOUND.value:
            raise Exception(
                f"[{name}] Service not found or inaccessible! Please ensure the service is running and the name is correct."
            )
        info_data = info.get("data")

        if info_data["type"] != expect_service_type.value:
            raise Exception(
                f"[{name}] Service type mismatch! The service is {info_data['type']}, but expected {expect_service_type.value}."
            )

        self._endpoint = self._get_endpoint(name, ip_address, info_data)
        if self._endpoint is None:
            raise Exception(f"[{name}] Service is not accessible for external client.")

    def _get_endpoint(self, name, service_ip, service_info):
        if service_ip == "localhost":
            if service_info["endpoint"]["ipc"] is not None:
                return f"ipc://{service_info['endpoint']['ipc']}"
            else:
                return f"tcp://{service_ip}:{service_info['endpoint']['tcp']}"
        else:
            if service_info["endpoint"]["tcp"] is not None:
                return f"tcp://{service_ip}:{service_info['endpoint']['tcp']}"
            else:
                print(
                    f"[{name}] in [{service_ip}] is inaccessible for external client."
                )
                return None


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
        self._context = context
        self._name = name
        self._ip_address = ip_address
        self._query_port = query_port
        self._message_class = message_class
        self._response_class = response_class
        self._timeout_ms = int(timeout * 1000.0)

        self._socket = None
        self._is_ready = self._init_socket()

    def _init_socket(self):
        """初始化或重置 Socket 狀態"""
        if self._socket:
            self._socket.close(linger=0)

        try:
            super().__init__(
                context=self._context,
                name=self._name,
                expect_service_type=ServiceType.RESPONDER,
                ip_address=self._ip_address,
                query_port=self._query_port,
            )
        except Exception as e:
            print(f"[{self._name}] Core initialization failed: {e}")
            self._is_ready = False
            return False

        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.REQ_RELAXED, 1)
        self._socket.setsockopt(zmq.REQ_CORRELATE, 1)

        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)
        self._is_ready = True
        return True

    def send(self, message):
        if not self._is_ready and not self._init_socket():
            return None

        if not isinstance(message, self._message_class):
            print(f"Warning: Expected {self._message_class.__name__}")
            return None

        try:
            message_bytes = ProtobufMessageHandler.serialize(message)
            self._socket.send(message_bytes)

            response_bytes = self._socket.recv()
            return ProtobufMessageHandler.deserialize(
                self._response_class, response_bytes
            )
        except zmq.Again:
            self._is_ready = False

        except zmq.ContextTerminated:
            self.close()

        except Exception as e:
            print(f"[{self._name}] Request error: {e}")
            self.close()
            raise Exception(e)

    def close(self):
        if self._socket:
            self._socket.close(linger=0)


class Publisher(ClientCore):
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        message_class,
        ip_address: str = "localhost",
        query_port: Optional[int] = Gateway.DEFAULT_QUERY_PORT,
    ):
        self._context = context
        self._name = name
        self._message_class = message_class
        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        try:
            super().__init__(
                context=self._context,
                name=self._name,
                expect_service_type=ServiceType.SUBSCRIBER,
                ip_address=ip_address,
                query_port=query_port,
            )
        except Exception as e:
            self._socket.close(linger=0)
            raise Exception(f"[{self._name}] Core initialization failed: {e}")
        self._socket.connect(self._endpoint)

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"[{self._name}] Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return
        message_bytes = ProtobufMessageHandler.serialize(message)
        self._socket.send(message_bytes)

    def close(self):
        self._socket.close()


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
        self._context = context
        self._name = name
        self._message_class = message_class
        self._callback = callback
        self._ip_address = ip_address
        self._query_port = query_port
        self._timeout = timeout * 1000.0  # 轉換為毫秒
        self._reconnect_attempts = 3
        self._buffer_size = buffer_size

        self._socket = None
        self._sub_stop_event = threading.Event()

        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _init_socket(self):
        """專門負責 Socket 的初始化與訂閱設定"""
        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVHWM, self._buffer_size)
        self._socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住

        try:
            super().__init__(
                context=self._context,
                name=self._name,
                expect_service_type=ServiceType.PUBLISHER,
                ip_address=self._ip_address,
                query_port=self._query_port,
            )
        except Exception as e:
            print(f"[{self._name}] Core initialization failed: {e}")
            return False

        self._socket.connect(self._endpoint)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        return True

    def _subscribe_process(self):
        if not self._init_socket():
            raise Exception(f"[{self._name}] Failed to initialize socket on startup.")

        while not self._sub_stop_event.is_set():
            try:
                # 1. 檢查資料，超時處理重連
                if not self._socket.poll(self._timeout):
                    # print(f"[{self._name}] Timeout, attempting to re-init...")
                    if not self._init_socket():
                        time.sleep(1)  # 重試間隔
                    continue

                # 2. 接收與反序列化
                message_bytes = self._socket.recv()
                message = ProtobufMessageHandler.deserialize(
                    self._message_class, message_bytes
                )
            except zmq.ContextTerminated:
                break
            except Exception as e:
                print(f"[{self._name}] Process Error: {e}")
                if self._sub_stop_event.is_set():
                    break

            try:
                self._callback(message)
            except Exception as e:
                print(f"[{self._name}] Callback execution error: {e}")

        # 離開迴圈後清理
        if self._socket:
            self._socket.close(linger=0)

    def close(self):
        self._sub_stop_event.set()
        if self._sub_thread:
            self._sub_thread.join(timeout=2.0)


class ActionClient(ClientCore):
    pass
