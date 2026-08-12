import threading
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
        self._heartbeat_timeout = heartbeat_timeout

        self._endpoint = None
        self._reconnect_event = threading.Event()
        # 取得 endpoint 後才會被設起來，讓建構流程可以「等待」而不是盲目 sleep
        self._endpoint_ready = threading.Event()
        self._gateway_reachable = True

        self._heartbeat_sock = self._create_heartbeat_socket()

        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._heartbeat_thread.start()

    def _create_heartbeat_socket(self):
        sock = self._context.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, int(self._heartbeat_timeout * 1000))
        sock.setsockopt(zmq.LINGER, 0)
        # Gateway 逾時或重啟後，REQ 會卡在「等待回覆」狀態，下一次 send 會直接 EFSM。
        # RELAXED / CORRELATE 允許直接重送並自動丟棄遲到的舊回覆。
        sock.setsockopt(zmq.REQ_RELAXED, 1)
        sock.setsockopt(zmq.REQ_CORRELATE, 1)
        sock.connect(f"tcp://{self._ip_address}:{self._query_port}")
        return sock

    def _reset_heartbeat_socket(self):
        try:
            self._heartbeat_sock.close(linger=0)
        except Exception:
            pass
        self._heartbeat_sock = self._create_heartbeat_socket()

    def _set_endpoint(self, endpoint: Optional[str]):
        """統一 endpoint 的狀態轉換，順便維護 _endpoint_ready / _reconnect_event"""
        if endpoint == self._endpoint:
            return
        if endpoint is None:
            self._endpoint = None
            self._endpoint_ready.clear()
        else:
            self._reconnect_event.set()
            self._endpoint = endpoint
            self._endpoint_ready.set()

    def wait_for_endpoint(self, timeout: float) -> bool:
        """等 gateway 回報 endpoint；逾時回傳 False，由呼叫端決定是否繼續。"""
        return self._endpoint_ready.wait(timeout)

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
            except zmq.Again:
                # Gateway 沒回應（還沒啟動或正在重啟）：保留最後已知的 endpoint 繼續運作，
                # 並繼續下一輪查詢。這裡若結束線程，gateway 回來之後就永遠不會再重連。
                if self._gateway_reachable:
                    print(f"[{self._name}] Gateway 無回應，持續重試中...")
                    self._gateway_reachable = False
                self._heartbeat_stop_event.wait(self._heartbeat_interval)
                continue
            except zmq.ContextTerminated:
                break
            except Exception as e:
                # 非預期錯誤同樣不能讓線程死掉，重建 socket 後重試
                print(f"[{self._name}] Heartbeat error: {e}")
                self._reset_heartbeat_socket()
                self._gateway_reachable = False
                self._heartbeat_stop_event.wait(self._heartbeat_interval)
                continue

            if not self._gateway_reachable:
                print(f"[{self._name}] Gateway 已恢復回應")
                self._gateway_reachable = True

            if resp is None or resp.get("status") != GatewayStatus.FOUND.value:
                # 服務不存在或名稱錯誤
                self._set_endpoint(None)
            elif resp.get("data", {}).get("type") != self._expect_service_type.value:
                # 名稱找到了但型別不符
                self._set_endpoint(None)
            else:
                # _get_endpoint 回 None 代表該服務不對外提供（例如只開了 ipc）
                self._set_endpoint(
                    _get_endpoint(self._ip_address, resp.get("data", {}))
                )

            self._heartbeat_stop_event.wait(self._heartbeat_interval)

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

    def _init_socket(self) -> bool:
        """初始化或重置 Socket 狀態；endpoint 尚未取得時回傳 False"""
        endpoint = self._endpoint
        if endpoint is None:
            return False

        if self._socket:
            self._socket.close(linger=0)

        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.REQ_RELAXED, 1)
        self._socket.setsockopt(zmq.REQ_CORRELATE, 1)

        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_ms)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(endpoint)
        self._reconnect_event.clear()
        return True

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(f"Warning: Expected {self._message_class.__name__}")
            return None

        if self._socket is None or self._reconnect_event.is_set():
            if not self._init_socket():
                print(
                    f"[{self._name}] No endpoint available. Please wait for the gateway to respond."
                )
                return None

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

    def _init_socket(self) -> bool:
        """初始化或重置 Socket 狀態；endpoint 尚未取得時回傳 False"""
        endpoint = self._endpoint
        if endpoint is None:
            return False

        if self._socket:
            self._socket.close(linger=0)

        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(endpoint)
        self._reconnect_event.clear()
        return True

    def send(self, message):
        if not isinstance(message, self._message_class):
            print(
                f"[{self._name}] Warning: Wrong message class, expect: {self._message_class.__name__}"
            )
            return

        if self._socket is None or self._reconnect_event.is_set():
            if not self._init_socket():
                print(
                    f"[{self._name}] No endpoint available. Please wait for the gateway to respond."
                )
                return

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
        connect_timeout: float = 5.0,
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
        # 等 gateway 回報 endpoint 再建 socket；等不到也不阻擋建構，
        # 訂閱線程會持續等待服務上線（原本是盲目 sleep 0.5 秒後直接 connect(None)）
        if not self.wait_for_endpoint(connect_timeout):
            print(
                f"[{self._name}] 尚未取得 endpoint（{connect_timeout}s），將在服務上線後自動連線"
            )

        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _init_socket(self) -> bool:
        """專門負責 Socket 的初始化與訂閱設定；endpoint 未就緒時回傳 False"""
        endpoint = self._endpoint
        if endpoint is None:
            return False

        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout)
        self._socket.setsockopt(zmq.CONFLATE, 1)
        self._socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
        self._socket.connect(endpoint)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._reconnect_event.clear()
        return True

    def _subscribe_process(self):
        waiting_logged = False
        while not self._sub_stop_event.is_set():
            try:
                if self._socket is None or self._reconnect_event.is_set():
                    if not self._init_socket():
                        if not waiting_logged:
                            print(
                                f"[{self._name}] No endpoint available. Waiting for the gateway to respond......"
                            )
                            waiting_logged = True
                        self._sub_stop_event.wait(0.5)
                        continue
                    waiting_logged = False

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
        connect_timeout: float = 5.0,
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

        # 等 gateway 回報 endpoint 再建 socket（原本盲目 sleep 0.5 秒後 connect(None)）
        if not self.wait_for_endpoint(connect_timeout):
            print(
                f"[{self._name}] 尚未取得 endpoint（{connect_timeout}s），將在服務上線後自動連線"
            )

        self._sub_thread = threading.Thread(target=self._subscribe_process, daemon=True)
        self._sub_thread.start()

    def _init_socket(self) -> bool:
        """專門負責 Socket 的初始化與訂閱設定；endpoint 未就緒時回傳 False"""
        endpoint = self._endpoint
        if endpoint is None:
            return False

        if self._socket:
            self._socket.close(linger=0)
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVTIMEO, self._timeout)
        self._socket.setsockopt(zmq.RCVHWM, 0)
        self._socket.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
        self._socket.connect(endpoint)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._reconnect_event.clear()
        return True

    def track_goal(self, goal_id: str, feedback_callback: callable):
        # 只是登記 callback，不需要等 socket 就緒（訂閱是收全部再依 goal_id 過濾）
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
        waiting_logged = False
        while not self._sub_stop_event.is_set():
            try:
                if self._socket is None or self._reconnect_event.is_set():
                    if not self._init_socket():
                        if not waiting_logged:
                            print(
                                f"[{self._name}] No endpoint available. Waiting for the gateway to respond......"
                            )
                            waiting_logged = True
                        self._sub_stop_event.wait(0.5)
                        continue
                    waiting_logged = False

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
        connect_timeout: float = 5.0,
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
        self._connect_timeout = connect_timeout
        self._goal_class = goal_class
        self._feedback_class = feedback_class
        self._result_class = result_class
        self._result_thread = None
        self.dealer_socket = None
        # DEALER socket 同時被使用者線程（send_goal / cancel_goal）與
        # result listener 線程使用，ZMQ socket 不是執行緒安全的，一律靠這把鎖序列化
        self._dealer_lock = threading.RLock()
        self._stop_event = threading.Event()

        # 追蹤處理中的任務 { goal_id: { "future": Future, "feedback_cb": Callable } }
        self.active_goals: Dict[str, Dict[str, Any]] = {}
        self.goals_lock = threading.Lock()

        # 等 gateway 回報 endpoint，而不是盲目 sleep 1 秒後 connect(None)
        if not self.wait_for_endpoint(connect_timeout):
            print(
                f"[{self._name}] 尚未取得 endpoint（{connect_timeout}s），將在服務上線後自動連線"
            )
        self._init_sockets()

    def _ensure_dealer(self) -> bool:
        """必要時建立/重建 DEALER socket。呼叫端必須持有 _dealer_lock。"""
        endpoint = self._endpoint
        if endpoint is None:
            return False

        if self.dealer_socket is None or self._reconnect_event.is_set():
            if self.dealer_socket is not None:
                self.dealer_socket.close(linger=0)
            sock = self._context.socket(zmq.DEALER)
            sock.setsockopt(zmq.LINGER, 0)  # 防止關閉時卡住
            sock.connect(endpoint)
            self.dealer_socket = sock
            self._reconnect_event.clear()
        return True

    def _init_sockets(self):
        # 1. DEALER Socket (發送 Goal/Cancel, 接收 Result) 改為延遲建立，
        #    endpoint 還沒到位時不會炸掉，服務上線後由 _ensure_dealer 補上
        with self._dealer_lock:
            self._ensure_dealer()

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
            connect_timeout=self._connect_timeout,
            heartbeat_interval=self._heartbeat_interval,
            heartbeat_timeout=self._heartbeat_timeout,
        )

        # 3. 啟動非同步監聽線程
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
        payload = ProtobufMessageHandler.serialize(goal)
        future = Future()

        with self._dealer_lock:
            if not self._ensure_dealer():
                raise ConnectionError(
                    f"[{self._name}] 尚未取得 action server 的 endpoint，無法送出 goal"
                )

            # 先登記追蹤再送出，避免 RESULT 比登記還早回來
            with self.goals_lock:
                self.active_goals[goal_id] = {"future": future}
            self._feedback_subscriber.track_goal(goal_id, feedback_callback)

            # 依照 Server 規範發送多幀訊息：
            # DEALER 自動加空幀，所以發出：[b"", b"GOAL", goal_id, payload]
            # Server 收到會是：[Routing_ID, b"", b"GOAL", goal_id, payload]
            self.dealer_socket.send_multipart(
                [b"", b"GOAL", goal_id.encode("utf-8"), payload]
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
        with self._dealer_lock:
            if not self._ensure_dealer():
                print(f"[{self._name} Client] Cancel failed: 尚未連線到 action server")
                return
            self.dealer_socket.send_multipart(
                [b"", b"CANCEL", goal_id.encode("utf-8")]
            )

    def _result_listener_loop(self):
        """監聽 DEALER Socket 回傳的最終任務結果"""
        while not self._stop_event.is_set():
            try:
                if self._feedback_timeout_flag.is_set():
                    print(
                        f"[{self._name} Client] Feedback receive timeout. Stopping result listener."
                    )
                    break

                # poll 與 recv 都必須在鎖內，否則會與 send_goal / cancel_goal
                # 併發操作同一個 socket；用短 timeout 避免長時間占住鎖
                frames = None
                with self._dealer_lock:
                    if self._ensure_dealer() and self.dealer_socket.poll(20):
                        frames = self.dealer_socket.recv_multipart()

                if frames is None:
                    self._stop_event.wait(0.01)  # 讓出鎖，避免餓死送出端
                    continue

                # 收到格式：[b"", b"RESULT", goal_id, status, payload]
                if len(frames) < 5:
                    continue

                msg_type = frames[1].decode("utf-8")
                if msg_type == "RESULT":
                    goal_id = frames[2].decode("utf-8")
                    status = frames[3].decode("utf-8")
                    result = ProtobufMessageHandler.deserialize(
                        self._result_class, frames[4]
                    )
                    with self.goals_lock:
                        entry = self.active_goals.pop(goal_id, None)

                    if entry is not None:
                        # 解除 SUB Socket 對該任務的追蹤，釋放資源
                        self._feedback_subscriber.untrack_goal(goal_id)
                        # set_result 會同步觸發使用者的 result_callback，
                        # 必須在鎖外執行，否則 callback 內再送 goal 就會死鎖
                        entry["future"].set_result({"status": status, "data": result})

            except zmq.ContextTerminated:
                break
            except zmq.ZMQError as e:
                if self._stop_event.is_set():
                    break
                print(f"[{self._name} Client] Result listener ZMQ error: {e}")
                self._stop_event.wait(0.1)
            except Exception as e:
                # 單筆訊息處理失敗不該讓整條 listener 消失
                print(f"[{self._name} Client] Result listener error: {e}")
                self._stop_event.wait(0.1)

        self._feedback_subscriber.close()
        with self._dealer_lock:
            if self.dealer_socket is not None:
                self.dealer_socket.close(linger=0)
                self.dealer_socket = None

    def close(self):
        for goal_id in list(self.active_goals.keys()):
            self.cancel_goal(goal_id)

        self._stop_event.set()
        if self._result_thread:
            self._result_thread.join(timeout=2.0)
        super().close()  # 停掉心跳線程，原本會遺留
