import socket
import threading
from typing import Any, Literal, Optional, Protocol, Tuple

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
            # --- 收 ---
            try:
                if not self._socket.poll(100):
                    continue

                message_bytes = self._socket.recv()
            except zmq.ContextTerminated:
                # 當 context 被關閉時，優雅退出
                print(f"[{self._name}] Context terminated.")
                break
            except zmq.ZMQError as e:
                if self._stop_event.is_set():
                    break
                print(f"[{self._name}] Receive error: {e}")
                continue

            # --- 處理 ---
            # REP socket 收到請求後「一定」要送出一次回應，
            # 否則狀態機會停在等待 send 的狀態，之後每次 recv 都會失敗。
            try:
                message = ProtobufMessageHandler.deserialize(
                    self._message_class, message_bytes
                )
            except Exception:
                print(
                    f"[{self._name}] Wrong message type, expect: {self._message_class.__name__}"
                )
                response_message = self._response_class()
            else:
                try:
                    response_message = self._callback(message)
                except Exception as e:
                    print(f"[{self._name}] Callback execution error: {e}")
                    response_message = self._response_class()

                if not isinstance(response_message, self._response_class):
                    print(
                        f"[{self._name}] Warning: Wrong message class, expect: {self._response_class.__name__}"
                    )
                    response_message = self._response_class()

            # --- 送 ---
            try:
                self._socket.send(ProtobufMessageHandler.serialize(response_message))
            except zmq.ContextTerminated:
                print(f"[{self._name}] Context terminated.")
                break
            except zmq.ZMQError as e:
                print(f"[{self._name}] Failed to send response: {e}")
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
        buffer_size: int = 1,
    ):
        try:
            super().__init__(context, name, ServiceType.PUBLISHER, port, mode)
        except Exception as e:
            raise Exception(f"[{name}] Core initialization failed: {e}")

        self._message_class = message_class

        self._mode = mode
        self._socket = context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.SNDHWM, buffer_size)
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

    def send_multipart(self, message_parts):
        self._socket.send_multipart(message_parts)

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
        self._socket.setsockopt(zmq.CONFLATE, 1)

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


class Broker(ServiceCore):
    def __init__(self, context, name, service_type, port=None, mode="tcp"):
        super().__init__(context, name, service_type, port, mode)
        """
        Broker is used to centralize the management of micro services in one port.
        """
        raise NotImplementedError("Broker is not implemented yet.")


class ActionGoal:
    def __init__(
        self,
        goal_id: str,
        goal_data: Any,
        feedback_class: Any,
        feedback_callback: callable,  # 傳入封裝好的 Publisher
        cancel_event: threading.Event,
    ):
        self.goal_id = goal_id
        self.goal = goal_data  # 用戶可以直接透過 goal_handle.goal 拿到 Protobuf 資料
        self._feedback_class = feedback_class
        self._feedback_callback = feedback_callback
        self._cancel_event = cancel_event

    def send_feedback(self, feedback_data: Any):
        if not isinstance(feedback_data, self._feedback_class):
            print(
                f"Warning: Wrong feedback class, expect: {self._feedback_class.__name__}"
            )
            return
        # 格式：[Goal_ID, 序列化後的 Feedback]
        self._feedback_callback(self.goal_id, feedback_data)

    def is_canceled(self) -> bool:
        """檢查 Client 是否發出了取消請求"""
        return self._cancel_event.is_set()


class ActionCallbackTemplate(Protocol):
    def __call__(self, goal_handle: ActionGoal) -> Tuple[Any, bool]:
        """
        強制約束：
        1. 必須接收一個名為 goal_handle 的參數
        2. 回傳值必須是一個 Tuple，裡面包含 (Result_Data, Success_Bool)
        """
        ...


class ActionServer(ServiceCore):
    def __init__(
        self,
        context: zmq.Context,
        name: str,
        goal_class: Any,
        feedback_class: Any,
        result_class: Any,
        callback: ActionCallbackTemplate,
        mode: Literal["tcp", "ipc", "both"] = "tcp",
        port: Optional[int] = None,
    ):
        try:
            super().__init__(context, name, ServiceType.ACTION, port, mode)
        except Exception as e:
            raise Exception(f"[{name}] Core initialization failed: {e}")

        self._context = context
        self._goal_class = goal_class
        self._feedback_class = feedback_class
        self._result_class = result_class
        self._execute_callback = callback
        self._accept_thread = None
        self._close_timeout = 5.0  # 關閉時等待 Worker 結束的最大時間

        # 1. 初始化 ROUTER Socket
        self._goal_socket = context.socket(zmq.ROUTER)
        self._goal_socket.setsockopt(zmq.LINGER, 0)

        # 💡 增加 Socket 執行緒鎖，保護 _goal_socket 不會在多執行緒下並發 send/recv
        self._socket_lock = threading.Lock()

        if self._enable_tcp:
            try:
                self._goal_socket.bind(f"tcp://*:{self._tcp_port}")
            except Exception as e:
                raise Exception(f"[{name}] Fail to bind {self._tcp_port}: {e}")

        if self._enable_ipc:
            try:
                self._goal_socket.bind(f"ipc://{self._ipc_path}")
            except Exception as e:
                raise Exception(f"[{name}] Fail to bind ipc://{self._ipc_path}: {e}")

        # 2. 初始化 Feedback Publisher
        self._feedback_pub_lock = threading.Lock()  # 增加 Feedback Publisher 的鎖
        self._feedback_pub = Publisher(
            context=context,
            name=f"{name}_feedback",
            message_class=feedback_class,
            mode=mode,
            buffer_size=0,  # 0 代表無限制
        )

        # 3. 用於追蹤當前正在執行的任務
        self.active_tasks = {}
        self.tasks_lock = threading.Lock()

        # 4. 啟動主要的監聽線程
        self._stop_event = threading.Event()
        self._accept_thread = threading.Thread(target=self._listener_loop, daemon=True)
        self._accept_thread.start()
        print(f"[{self._name}] Action Server started successfully.")

    def close(self):
        self._stop_event.set()
        if self._accept_thread:
            self._accept_thread.join()

        self._feedback_pub.close()

    def _listener_loop(self):
        """ROUTER 監聽迴圈：只負責收單與分發"""
        while not self._stop_event.is_set():
            try:
                # 使用 poller 避免 recv 阻塞，讓 stop_event 能生效
                if not self._goal_socket.poll(100, zmq.POLLIN):
                    continue

                # 💡 接收時加上 Lock 保護
                with self._socket_lock:
                    frames = self._goal_socket.recv_multipart()

                if len(frames) < 4:
                    continue

                routing_id = frames[0]
                msg_type = frames[2].decode("utf-8")
                goal_id = frames[3].decode("utf-8")
                if msg_type == "GOAL":
                    if len(frames) < 5:
                        continue
                    try:
                        goal_data = ProtobufMessageHandler.deserialize(
                            self._goal_class, frames[4]
                        )
                    except Exception:
                        print(
                            f"[{self._name}] Wrong message type, expect: {self._goal_class.__name__}"
                        )
                        continue

                    self._handle_new_goal(routing_id, goal_id, goal_data)

                elif msg_type == "CANCEL":
                    print(
                        f"[{self._name}] Received cancel request for goal_id: {goal_id}"
                    )
                    self._handle_cancel_request(goal_id)

            except zmq.ZMQError as e:
                if self._stop_event.is_set():
                    break
                print(f"[{self._name}] ZMQ Error in listener: {e}")
            except Exception as e:
                print(f"[{self._name}] Unexpected error in listener: {e}")

        # 發送取消信號給所有 Worker，並等待它們結束
        task_ids = list(self.active_tasks.keys())
        for task_id in task_ids:
            if task_id in self.active_tasks:
                self.active_tasks[task_id]["cancel_event"].set()
                self.active_tasks[task_id]["worker"].join(timeout=self._close_timeout)

        # 關閉處理
        if hasattr(self, "_heartbeat_client") and self._heartbeat_client:
            self._heartbeat_client.stop()

        with self._socket_lock:
            if self._goal_socket:
                self._goal_socket.close(linger=0)

    def _handle_new_goal(self, routing_id: bytes, goal_id: str, goal_data: Any):
        """收到新任務：建立取消訊號，並交給獨立的 Worker 線程處理"""
        cancel_event = threading.Event()

        with self.tasks_lock:
            self.active_tasks[goal_id] = {
                "cancel_event": cancel_event,
                "routing_id": routing_id,
            }
            # 啟動非同步 Worker
            worker = threading.Thread(
                target=self._worker_launcher,
                args=(routing_id, goal_id, goal_data, cancel_event),
                daemon=True,
            )
            worker.start()
            self.active_tasks[goal_id]["worker"] = worker

    def _handle_cancel_request(self, goal_id: str):
        """收到取消請求：拉動對應任務的 Event 警報"""
        with self.tasks_lock:
            if goal_id in self.active_tasks:
                self.active_tasks[goal_id]["cancel_event"].set()

    def _pub_feedback(self, goal_id: str, feedback_data: Any):
        with self._feedback_pub_lock:
            self._feedback_pub.send_multipart(
                [
                    goal_id.encode("utf-8"),
                    ProtobufMessageHandler.serialize(feedback_data),
                ]
            )

    def _worker_launcher(
        self,
        routing_id: bytes,
        goal_id: str,
        goal_data: Any,
        cancel_event: threading.Event,
    ):
        """Worker 執行緒：封裝 ActionGoal 並調用用戶 Callback"""

        # 💡 將所有控制項封裝進 ActionGoal 實例中
        goal_handle = ActionGoal(
            goal_id=goal_id,
            goal_data=goal_data,
            feedback_class=self._feedback_class,
            feedback_callback=self._pub_feedback,
            cancel_event=cancel_event,
        )

        # 執行用戶傳進來的 callback
        try:
            result_data, success = self._execute_callback(goal_handle)
        except Exception:
            # 這裡建議建立一個錯誤的結果 Protobuf 實例，此處先以文字或通用處理模擬
            result_data = self._result_class()
            success = False

        # 判定最終回傳給 Client 的狀態
        status_str = "SUCCEEDED" if success else "FAILED"
        if cancel_event.is_set():
            status_str = "CANCELED"

        # 透過 ROUTER 回傳結果
        # 💡 關鍵修正：發送時必須進 Lock，防止與 listener_loop 衝突
        try:
            with self._socket_lock:
                self._goal_socket.send_multipart(
                    [
                        routing_id,
                        b"",
                        b"RESULT",
                        goal_id.encode("utf-8"),
                        status_str.encode("utf-8"),
                        ProtobufMessageHandler.serialize(result_data),
                    ]
                )
        except zmq.ZMQError as e:
            print(f"[{self._name}] Failed to send result to client: {e}")

        # 清理任務追蹤
        with self.tasks_lock:
            if goal_id in self.active_tasks:
                del self.active_tasks[goal_id]
