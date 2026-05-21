import threading
import time
from typing import Optional

import zmq

from lemegeton.gateway import Gateway, GatewayStatus, ServiceType
from lemegeton.serializer import ProtobufMessageHandler

CLIENT_HEARTBEAT_INTERVAL = 0.5  # seconds
CLIENT_HEARTBEAT_TIMEOUT = 2.0  # seconds


class ClientCore:
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        expect_service_type: ServiceType,
        ip_address: str,
        query_port: int,
        heartbeat_interval: float = CLIENT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = CLIENT_HEARTBEAT_TIMEOUT,
    ):
        self._context = context
        self._name = name
        self._expect_service_type = expect_service_type
        self._ip_address = ip_address
        self._query_port = query_port
        self._heartbeat_interval = heartbeat_interval

        self._endpoint = None
        self._reconnect_event = threading.Event()

        self._heartbeat_sock = context.socket(zmq.REQ)
        self._heartbeat_sock.setsockopt(zmq.RCVTIMEO, int(heartbeat_timeout * 1000))
        self._heartbeat_sock.setsockopt(zmq.LINGER, 0)
        self._heartbeat_sock.connect(f"tcp://{ip_address}:{self._query_port}")

        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat(self):
        def _get_endpoint(service_ip, service_info):
            if service_ip == "localhost":
                if service_info.get("endpoint", {}).get("ipc") is not None:
                    return f"ipc://{service_info['endpoint']['ipc']}"
                else:
                    return f"tcp://{service_ip}:{service_info['endpoint']['tcp']}"
            else:
                if service_info.get("endpoint", {}).get("tcp") is not None:
                    return f"tcp://{service_ip}:{service_info['endpoint']['tcp']}"
                else:
                    return None

        while not self._heartbeat_stop_event.is_set():
            try:
                self._heartbeat_sock.send_json({"name": self._name})
                resp = self._heartbeat_sock.recv_json()
                if resp is None or resp.get("status") != GatewayStatus.FOUND.value:
                    # print(
                    #     f"[{self._name}] Service not found or inaccessible! Please ensure the service is running and the name is correct."
                    # )
                    self._endpoint = None
                    time.sleep(self._heartbeat_interval)
                    continue

                elif (
                    resp.get("data", {}).get("type") != self._expect_service_type.value
                ):
                    # print(
                    #     f"[{self._name}] Service type mismatch! The service is {resp.get('data', {}).get('type')}, but expected {self._expect_service_type.value}."
                    # )
                    self._endpoint = None
                    time.sleep(self._heartbeat_interval)
                    continue

                else:
                    endpoint = _get_endpoint(self._ip_address, resp.get("data", {}))
                    if endpoint is None:
                        # print(
                        #     f"[{self._name}] Service is not accessible for external client."
                        # )
                        self._endpoint = None

                    elif endpoint != self._endpoint:
                        # print(
                        #     f"[{self._name}] Service endpoint changed: {self._endpoint} -> {endpoint}"
                        # )
                        self._reconnect_event.set()
                        self._endpoint = endpoint
                    time.sleep(self._heartbeat_interval)

            except zmq.Again:
                print("Gateway query timeout. Retrying...")
            except zmq.ContextTerminated:
                break
            except Exception as e:
                print(f"[{self._name}] Heartbeat error: {e}")
                break

        self._heartbeat_sock.close(linger=0)

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
        heartbeat_interval: float = CLIENT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = CLIENT_HEARTBEAT_TIMEOUT,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.RESPONDER,
            ip_address=ip_address,
            query_port=query_port,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )

        self._message_class = message_class
        self._response_class = response_class
        self._timeout_ms = int(timeout * 1000.0)

        self._socket = None

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
        self._reconnect_event.clear()

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(f"Warning: Expected {self._message_class.__name__}")
            return None

        if self._endpoint is None:
            print(
                f"[{self._name}] No endpoint available. Please wait for the gateway to respond."
            )
            return None

        elif self._reconnect_event.is_set():
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
        return self._endpoint is not None and not self._reconnect_event.is_set()

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
        heartbeat_interval: float = CLIENT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = CLIENT_HEARTBEAT_TIMEOUT,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.SUBSCRIBER,
            ip_address=ip_address,
            query_port=query_port,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )

        self._message_class = message_class

        self._socket = None

    def _init_socket(self):
        """初始化或重置 Socket 狀態"""
        if self._socket:
            self._socket.close(linger=0)

        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)
        self._last_endpoint = self._endpoint
        self._reconnect_event.clear()

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"[{self._name}] Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return

        if self._endpoint is None:
            print(
                f"[{self._name}] No endpoint available. Please wait for the gateway to respond."
            )
            return
        elif self._reconnect_event.is_set():
            self._init_socket()

        try:
            message_bytes = ProtobufMessageHandler.serialize(message)
            self._socket.send(message_bytes)
        except Exception as e:
            print(f"[{self._name}] Error sending message: {e}")

    def is_connect(self):
        return self._endpoint is not None and not self._reconnect_event.is_set()

    def close(self):
        if self._socket:
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
        heartbeat_interval: float = CLIENT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = CLIENT_HEARTBEAT_TIMEOUT,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.PUBLISHER,
            ip_address=ip_address,
            query_port=query_port,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
        self._message_class = message_class
        self._callback = callback
        self._timeout = int(timeout * 1000)  # 轉換為毫秒
        self._sub_stop_event = threading.Event()
        self._sub_thread = None

        self._socket = None
        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _init_socket(self):
        """專門負責 Socket 的初始化與訂閱設定"""
        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout)
        self._socket.setsockopt(zmq.CONFLATE, 1)
        self._socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
        self._socket.connect(self._endpoint)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._reconnect_event.clear()

    def _subscribe_process(self):
        while not self._sub_stop_event.is_set():
            try:
                if self._endpoint is None:
                    print(
                        f"[{self._name}] No endpoint available. Waiting for the gateway to respond......"
                    )
                    time.sleep(0.5)
                    continue
                elif self._reconnect_event.is_set():
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
        return self._endpoint is not None and not self._reconnect_event.is_set()

    def close(self):
        self._sub_stop_event.set()
        if self._sub_thread:
            self._sub_thread.join(timeout=2.0)


class ActionClient(ClientCore):
    pass
