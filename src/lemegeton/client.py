import threading
import time
import uuid
from concurrent.futures import Future
from typing import Any, Dict, Optional

import zmq

from lemegeton.gateway import Gateway, GatewayStatus, ServiceType
from lemegeton.serializer import ProtobufMessageHandler

CLIENT_HEARTBEAT_INTERVAL = 0.5  # seconds
CLIENT_HEARTBEAT_TIMEOUT = 1.0  # seconds


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
        timeout: float = 30.0,
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
        time.sleep(0.5)  # 等待心跳確認服務狀態
        self._init_socket()

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


class ActionFeedbackSubscriber(ClientCore):
    def __init__(
        self,
        context: zmq.Context,
        timeout_flag: threading.Event,
        name: str,
        feedback_class,
        ip_address: str = "localhost",
        query_port: Optional[int] = Gateway.DEFAULT_QUERY_PORT,
        timeout: float = 30.0,
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
        self._feedback_class = feedback_class
        self._timeout = int(timeout * 1000)  # 轉換為毫秒
        self._sub_stop_event = threading.Event()

        self.active_goals = {}
        self.goals_lock = threading.Lock()
        self._socket = None
        self._timeout_flag = timeout_flag

        time.sleep(0.5)  # 等待心跳確認服務狀態
        self._init_socket()

        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _init_socket(self):
        """專門負責 Socket 的初始化與訂閱設定"""
        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout)
        self._socket.setsockopt(zmq.RCVHWM, 0)
        self._socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
        self._socket.connect(self._endpoint)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._reconnect_event.clear()

    def track_goal(self, goal_id: str, feedback_callback: callable):
        while self._socket is None:
            print(
                f"[{self._name}] Socket not initialized yet. Waiting before tracking goal {goal_id}..."
            )
            time.sleep(0.5)

        if goal_id in self.active_goals:
            print(f"[{self._name}] Goal {goal_id} is already being tracked.")
            return
        with self.goals_lock:
            self.active_goals[goal_id] = {"callback": feedback_callback}
            # self._socket.setsockopt_string(zmq.SUBSCRIBE, goal_id)

    def untrack_goal(self, goal_id: str):
        with self.goals_lock:
            if goal_id in self.active_goals:
                # self._socket.setsockopt_string(zmq.UNSUBSCRIBE, goal_id)
                del self.active_goals[goal_id]

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

                # SUB 接收格式：[goal_id, feedback_payload]
                frames = self._socket.recv_multipart()
                if len(frames) < 2:
                    print(
                        f"[{self._name}] Received malformed feedback message. Ignoring."
                    )
                    continue

                goal_id = frames[0].decode("utf-8")

                with self.goals_lock:
                    if (
                        goal_id in self.active_goals
                        and self.active_goals[goal_id]["callback"]
                    ):
                        feedback = ProtobufMessageHandler.deserialize(
                            self._feedback_class, frames[1]
                        )
                        self.active_goals[goal_id]["callback"](feedback)
                    else:
                        # print(
                        #     f"[{self._name}] Received feedback for untracked goal {goal_id}. Ignoring."
                        # )
                        pass

            except zmq.Again:
                # 沒有 feedback 只代表目前閒置（沒有進行中的任務），不是錯誤。
                # 服務真的離線會由心跳把 _endpoint 設為 None，由上面的分支處理。
                continue
            except zmq.ContextTerminated:
                break
            except Exception as e:
                print(f"[{self._name}] Process Error: {e}")
                if self._sub_stop_event.is_set():
                    break
                continue

        # 非主動關閉卻離開迴圈，代表 feedback 通道已無法使用，
        # 通知 ActionClient 一併結束 result listener
        if not self._sub_stop_event.is_set():
            self._timeout_flag.set()

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
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        goal_class,
        feedback_class,
        result_class,
        ip_address: str = "localhost",
        timeout: float = 30.0,
        query_port: Optional[int] = Gateway.DEFAULT_QUERY_PORT,
        heartbeat_interval: float = CLIENT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = CLIENT_HEARTBEAT_TIMEOUT,
    ):
        super().__init__(
            context=context,
            name=name,
            expect_service_type=ServiceType.ACTION,
            ip_address=ip_address,
            query_port=query_port,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
        self._context = context
        self._query_port = query_port
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._timeout = timeout
        self._goal_class = goal_class
        self._feedback_class = feedback_class
        self._result_class = result_class
        self._result_thread = None

        # 追蹤處理中的任務 { goal_id: { "future": Future, "feedback_cb": Callable } }
        self.active_goals: Dict[str, Dict[str, Any]] = {}
        self.goals_lock = threading.Lock()

        time.sleep(1.0)  # 等待心跳確認服務狀態
        self._init_sockets()

    def _init_sockets(self):
        # 1. 初始化 DEALER Socket (發送 Goal/Cancel, 接收 Result)
        self.dealer_socket = self._context.socket(zmq.DEALER)
        self.dealer_socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
        self.dealer_socket.connect(self._endpoint)

        # 2. 初始化 SUB Socket (接收 Feedback)
        self._feedback_timeout_flag = threading.Event()
        self._feedback_subscriber = ActionFeedbackSubscriber(
            context=self._context,
            timeout_flag=self._feedback_timeout_flag,
            name=f"{self._name}_feedback",
            feedback_class=self._feedback_class,
            ip_address=self._ip_address,
            query_port=self._query_port,
            timeout=self._timeout,
            heartbeat_interval=self._heartbeat_interval,
            heartbeat_timeout=self._heartbeat_timeout,
        )

        # 3. 啟動非同步監聽線程
        self._stop_event = threading.Event()
        self._result_thread = threading.Thread(
            target=self._result_listener_loop, daemon=True
        )
        self._result_thread.start()

    def send_goal(
        self,
        goal: Any,
        feedback_callback: callable,
        result_callback: callable,
        cancel_callback: Optional[callable] = None,
    ) -> tuple[str, Future]:
        """
        發送新任務 (非同步，不阻塞)
        :param goal: 任務內容
        :return: (goal_id, Future 物件)
        """

        if not isinstance(goal, self._goal_class):
            print(
                f"[{self._name} Client] Warning: Wrong goal class, expect: {self._goal_class.__name__}"
            )
            raise ValueError(f"Wrong goal class, expect: {self._goal_class.__name__}")

        goal_id = str(uuid.uuid4())
        future = Future()

        with self.goals_lock:
            self.active_goals[goal_id] = {"future": future}

        # 讓 SUB Socket 動態訂閱這個特定 goal_id 的進度，過濾掉別人的進度
        self._feedback_subscriber.track_goal(goal_id, feedback_callback)

        # 依照 Server 規範發送多幀訊息：
        # DEALER 自動加空幀，所以發出：[b"", b"GOAL", goal_id, payload]
        # Server 收到會是：[Routing_ID, b"", b"GOAL", goal_id, payload]
        self.dealer_socket.send_multipart(
            [
                b"",
                b"GOAL",
                goal_id.encode("utf-8"),
                ProtobufMessageHandler.serialize(goal),
            ]
        )

        def _callback_wrapper(fut):
            try:
                result_dict = fut.result()
                if result_dict["status"] == "CANCELED":
                    if cancel_callback:
                        cancel_callback(result_dict["status"], result_dict["data"])
                    else:
                        print(
                            f"[{self._name} Client] Goal {goal_id} was canceled. No cancel callback provided."
                        )
                else:
                    result_callback(result_dict["status"], result_dict["data"])

            except Exception as e:
                print(f"[{self._name} Client] Result callback error: {e}")

        future.add_done_callback(_callback_wrapper)
        return goal_id, future

    def cancel_goal(self, goal_id: str):
        """向 Server 發送取消任務請求"""
        with self.goals_lock:
            if goal_id not in self.active_goals:
                print(
                    f"[{self._name} Client] Cancel failed: Goal {goal_id} not found or already finished."
                )
                return

        # 發送取消訊息：[b"", b"CANCEL", goal_id]
        self.dealer_socket.send_multipart([b"", b"CANCEL", goal_id.encode("utf-8")])

    def _result_listener_loop(self):
        """監聽 DEALER Socket 回傳的最終任務結果"""
        while not self._stop_event.is_set():
            try:
                if self._feedback_timeout_flag.is_set():
                    print(
                        f"[{self._name} Client] Feedback receive timeout. Stopping result listener."
                    )
                    break

                if not self.dealer_socket.poll(100):
                    continue

                # DEALER 接收會自動剝掉空幀，收到格式：[b"RESULT", goal_id, status, payload]
                frames = self.dealer_socket.recv_multipart()
                if len(frames) < 4:
                    continue

                msg_type = frames[1].decode("utf-8")
                if msg_type == "RESULT":
                    goal_id = frames[2].decode("utf-8")
                    status = frames[3].decode("utf-8")
                    result = ProtobufMessageHandler.deserialize(
                        self._result_class, frames[4]
                    )
                    with self.goals_lock:
                        if goal_id in self.active_goals:
                            # 解除 SUB Socket 對該任務的訂閱，釋放資源
                            self._feedback_subscriber.untrack_goal(goal_id)

                            # 把結果塞入 Future，通知等待的線程
                            result_dict = {"status": status, "data": result}
                            self.active_goals[goal_id]["future"].set_result(result_dict)

                            # # 移除追蹤
                            del self.active_goals[goal_id]

            except zmq.ZMQError:
                break
        self._feedback_subscriber.close()
        self.dealer_socket.close()

    def close(self):
        for goal_id in list(self.active_goals.keys()):
            self.cancel_goal(goal_id)

        self._stop_event.set()
        if self._result_thread:
            self._result_thread.join(timeout=2.0)
