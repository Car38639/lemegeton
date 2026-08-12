import json
import threading
import time
import uuid
from enum import Enum
from typing import Optional

import redis
import zmq


class Context(zmq.Context):
    """ZeroMQ Context 的簡單封裝，保證一致性"""

    def __init__(self):
        super().__init__()


class GatewayStatus(Enum):
    SUCCESS = "SUCCESS"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    MISMATCH = "MISMATCH"

    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"

    INVALID_FORMAT = "INVALID_FORMAT"


class ServiceType(Enum):
    PUBLISHER = "pub"
    SUBSCRIBER = "sub"
    REQUESTER = "req"
    RESPONDER = "res"
    PUSHER = "push"
    PULLER = "pull"
    ACTION = "action"


class RegistryAction(Enum):
    REGISTER = "register"
    UNREGISTER = "unregister"


class Gateway:
    REGISTRY_PATH = "@lemegeton_registry"
    HEARTBEAT_PATH = "@lemegeton_heartbeat"
    HEARTBEAT_RATE = 10  # 每 10 秒發送一次心跳
    DEFAULT_QUERY_PORT = 60001

    def __init__(
        self, port: Optional[int] = None, redis_conf={"host": "localhost", "port": 6379}
    ):
        self.r = redis.Redis(**redis_conf, decode_responses=True)
        self.local_cache = {}  # 格式: {name: {"data": dict, "last_seen": float}}
        self.context = zmq.Context()
        self.ttl = 2 * Gateway.HEARTBEAT_RATE  # 設定2倍心跳的秒數為過期門檻

        self.registry_sock = self.context.socket(zmq.REP)
        self.registry_sock.bind(f"ipc://{Gateway.REGISTRY_PATH}")

        self.heartbeat_sock = self.context.socket(zmq.REP)
        self.heartbeat_sock.bind(f"ipc://{Gateway.HEARTBEAT_PATH}")

        # --- 2. TCP 查詢 Socket ---
        self.query_sock = self.context.socket(zmq.REP)
        if port is None:
            port = Gateway.DEFAULT_QUERY_PORT
        self.query_sock.bind(f"tcp://*:{port}")

        # --- 3. Poller 設定 ---
        self.poller = zmq.Poller()
        self.poller.register(self.registry_sock, zmq.POLLIN)
        self.poller.register(self.heartbeat_sock, zmq.POLLIN)
        self.poller.register(self.query_sock, zmq.POLLIN)

        self.last_sync_time = time.time()

    def sync_and_cleanup(self):
        """同步數據至 Redis 並清理本地過期快取"""
        now = time.time()
        stale_keys = []

        # 找出已過期的 Key
        for name, info in self.local_cache.items():
            if now - info["last_seen"] > self.ttl:
                stale_keys.append(name)

        # 執行清理
        for name in stale_keys:
            del self.local_cache[name]
            # 同步刪除 Redis 中的資料
            self.r.delete(f"svc:{name}")
            print(f"[Cleanup] 服務 {name} 已過期，執行清理")

        # 同步剩餘的活躍數據至 Redis
        if self.local_cache:
            pipe = self.r.pipeline()
            for name, info in self.local_cache.items():
                pipe.set(f"svc:{name}", json.dumps(info), ex=self.ttl)
            pipe.execute()
            print(f"[Sync] 同步 {len(self.local_cache)} 個活躍服務至 Redis")

        self.last_sync_time = now

    def run(self):
        print("Gateway 啟動成功，進入監聽狀態...")
        try:
            while True:
                socks = dict(self.poller.poll(1000))
                # 處理 Unregister (寫入)
                if self.registry_sock in socks:
                    msg = self.registry_sock.recv_json()
                    print(f"[Registry] 收到註冊請求: {msg}")

                    action = msg.get("action")
                    name = msg.get("name")
                    request_service_id = msg.get("service_id")

                    if action == RegistryAction.REGISTER.value:
                        current_service = self.local_cache.get(name)
                        # 只有「還活著」的舊條目才擋新註冊：崩潰的服務不會註銷,
                        # 心跳斷了超過 TTL 就視同過期,直接讓新身份接管,
                        # 不必等 sync_and_cleanup 剛好跑到。
                        if current_service and (
                            time.time() - current_service.get("last_seen", 0) > self.ttl
                        ):
                            print(f"[Registry] 服務 {name} 的舊註冊已過期，允許接管")
                            current_service = None
                        if (
                            current_service
                            and current_service.get("service_id") != request_service_id
                        ):
                            print(
                                f"[Warning] 服務名稱 '{name}' 已被 {current_service.get('service_id')} 占用！來自 {request_service_id} 的註冊請求被拒絕。"
                            )
                            # 名稱已被占用，且 ID 不同
                            self.registry_sock.send_json(
                                {"status": GatewayStatus.ALREADY_EXISTS.value}
                            )
                        else:
                            # 新註冊
                            self.local_cache[name] = {
                                "data": msg.get("data"),
                                "service_id": request_service_id,  # 紀錄擁有者 ID
                                "last_seen": time.time(),
                            }
                            self.registry_sock.send_json(
                                {"status": GatewayStatus.SUCCESS.value}
                            )
                    elif action == RegistryAction.UNREGISTER.value:
                        current_service = self.local_cache.get(name)
                        if (
                            current_service
                            and current_service.get("service_id") == request_service_id
                        ):
                            del self.local_cache[name]
                            self.r.delete(f"svc:{name}")
                            print(f"[Unregister] 服務 {name} 已被註銷")
                            self.registry_sock.send_json(
                                {"status": GatewayStatus.SUCCESS.value}
                            )
                        else:
                            print(
                                f"[Warning] 嘗試註銷服務 '{name}' 失敗！請求 ID {request_service_id} 與現有服務 ID 不匹配或服務不存在。"
                            )
                            self.registry_sock.send_json(
                                {"status": GatewayStatus.NOT_FOUND.value}
                            )

                # 處理 IPC 心跳 (寫入)
                if self.heartbeat_sock in socks:
                    msg = self.heartbeat_sock.recv_json()
                    print(f"[Heartbeat] 收到心跳請求: {msg}")

                    name = msg.get("name")
                    new_service_id = msg.get(
                        "service_id"
                    )  # Client 端啟動時產生的唯一 ID

                    current_service = self.local_cache.get(name)

                    if not current_service:
                        # 本地無此服務，從 Redis 查詢資料
                        val = self.r.get(f"svc:{name}")
                        if val:
                            info = json.loads(val)
                            if info.get("service_id") != new_service_id:
                                print(
                                    f"[Warning] 服務名稱 '{name}' ID 不符合。從 Redis 查詢到的 ID 與心跳請求的 ID 不匹配。"
                                )
                                self.heartbeat_sock.send_json(
                                    {"status": GatewayStatus.MISMATCH.value}
                                )
                            else:
                                self.local_cache[name] = info
                                print(
                                    f"[Heartbeat] 服務 '{name}' 從 Redis 回填至本地快取，ID: {new_service_id}"
                                )
                                self.heartbeat_sock.send_json(
                                    {"status": GatewayStatus.SUCCESS.value}
                                )
                        else:
                            print(
                                f"[Heartbeat] 服務 '{name}' 不存在於本地快取和 Redis 中，將其註冊到本地快取，ID: {new_service_id}"
                            )
                            self.local_cache[name] = {
                                "data": msg.get("data"),
                                "service_id": new_service_id,
                                "last_seen": time.time(),
                            }
                            self.heartbeat_sock.send_json(
                                {"status": GatewayStatus.SUCCESS.value}
                            )

                    elif current_service.get("service_id") != new_service_id:
                        print(f"[Warning] 服務名稱 '{name}' ID 不符合。")
                        # ID 不匹配，可能是名稱被占用或服務重啟但 ID 變了
                        self.heartbeat_sock.send_json(
                            {"status": GatewayStatus.MISMATCH.value}
                        )
                    else:
                        # 正常心跳，更新心跳時間戳和資料
                        self.local_cache[name]["last_seen"] = time.time()
                        self.heartbeat_sock.send_json(
                            {"status": GatewayStatus.SUCCESS.value}
                        )

                # 處理 TCP 查詢 (讀取)
                if self.query_sock in socks:
                    msg = self.query_sock.recv_json()
                    name = msg.get("name")

                    res_info = self.local_cache.get(name)
                    if not res_info:
                        # 本地無則查 Redis。Redis 內的格式與 local_cache 相同：
                        # {"data": ..., "service_id": ..., "last_seen": ...}
                        # 回填時必須保持同一層級，否則後續查詢會多包一層而失效
                        val = self.r.get(f"svc:{name}")
                        if val:
                            res_info = json.loads(val)
                            res_info.setdefault("last_seen", time.time())
                            self.local_cache[name] = res_info
                            print(f"[Query] 服務 '{name}' 從 Redis 回填至本地快取")

                    if res_info and res_info.get("data"):
                        self.query_sock.send_json(
                            {
                                "status": GatewayStatus.FOUND.value,
                                "data": res_info["data"],
                            }
                        )
                    else:
                        self.query_sock.send_json(
                            {"status": GatewayStatus.NOT_FOUND.value}
                        )

                # 每 5 秒執行一次同步與清理
                if time.time() - self.last_sync_time > 5:
                    self.sync_and_cleanup()

        except KeyboardInterrupt:
            print("\nServer is shutting down...")
        finally:
            self.query_sock.close()
            self.registry_sock.close()
            self.heartbeat_sock.close()
            self.context.term()


class HeartbeatClient:
    def __init__(
        self,
        context,
        name,
        data,
        registry_path=Gateway.REGISTRY_PATH,
        heartbeat_path=Gateway.HEARTBEAT_PATH,
    ):
        self.context = context
        self.registry_path = registry_path
        self.heartbeat_path = heartbeat_path
        self.name = name
        self.data = data
        self.service_id = str(uuid.uuid4())  # 生成本次啟動的唯一身份標籤
        # 首次註冊失敗多半是暫時的:服務快速重啟時,gateway 裡上一世的註冊
        # 要到 TTL(2倍心跳)後才過期;或 gateway 本身還沒起來。這兩種都會
        # 自行解除,所以重試到略超過 TTL 再放棄,而不是一次失敗就讓整個
        # 服務 crash 進 restart loop。
        deadline = time.time() + 2 * Gateway.HEARTBEAT_RATE + 5
        while not self._register_service():
            self.registry_sock.close()
            self.heartbeat_sock.close()
            if time.time() >= deadline:
                raise Exception(f"[{self.name}] Service registration failed")
            print(f"[{self.name}] Registration not accepted yet; retrying...")
            time.sleep(2.0)

        self._heartbeat_event = threading.Event()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True
        )
        self._heartbeat_thread.start()

    def _reset_sockets(self):
        self.registry_sock.close()
        self.heartbeat_sock.close()
        return self._register_service()

    def _register_service(self):
        self.registry_sock = self.context.socket(zmq.REQ)
        # 設定接收超時，避免死鎖
        self.registry_sock.setsockopt(zmq.RCVTIMEO, 3000)
        # 確保關閉時不阻塞
        self.registry_sock.setsockopt(zmq.LINGER, 0)
        # 允許在還沒recv的情況下再次 send
        self.registry_sock.setsockopt(zmq.REQ_RELAXED, 1)
        # 自動關聯請求與回覆，避免舊回覆干擾新請求
        self.registry_sock.setsockopt(zmq.REQ_CORRELATE, 1)
        self.registry_sock.connect(f"ipc://{self.registry_path}")

        self.heartbeat_sock = self.context.socket(zmq.REQ)
        # 設定接收超時，避免死鎖
        self.heartbeat_sock.setsockopt(zmq.RCVTIMEO, 1000)
        # 確保關閉時不阻塞
        self.heartbeat_sock.setsockopt(zmq.LINGER, 0)
        # 允許在還沒recv的情況下再次 send
        self.heartbeat_sock.setsockopt(zmq.REQ_RELAXED, 1)
        # 自動關聯請求與回覆，避免舊回覆干擾新請求
        self.heartbeat_sock.setsockopt(zmq.REQ_CORRELATE, 1)
        self.heartbeat_sock.connect(f"ipc://{self.heartbeat_path}")
        try:
            self.registry_sock.send_json(
                {
                    "action": RegistryAction.REGISTER.value,
                    "name": self.name,
                    "service_id": self.service_id,
                    "data": self.data,
                }
            )
            resp = self.registry_sock.recv_json()

            if resp.get("status") == GatewayStatus.ALREADY_EXISTS.value:
                # print(
                #     f"[{self.name}] Service '{self.name}' already exists with a different ID."
                # )
                return False
            else:
                return True
        except zmq.Again:
            # print(f"[{self.name}][Error] Gateway 無回應!")
            return False

    def _heartbeat_loop(self):
        while not self._heartbeat_event.is_set():
            try:
                self.heartbeat_sock.send_json(
                    {
                        "name": self.name,
                        "service_id": self.service_id,
                        "data": self.data,
                    }
                )
                resp = self.heartbeat_sock.recv_json()

                if resp.get("status") == GatewayStatus.ALREADY_EXISTS.value:
                    # print(
                    #     f"[{self.name}]  ID mismatch detected. Another service with the same name is active."
                    # )

                    self._heartbeat_event.set()  # 停止心跳，避免干擾他人
                    self.heartbeat_sock.close()
                    break
            except zmq.Again:
                # print(f"[{self.name}] Heartbeat timeout. No response from Gateway.")
                # if self._reset_sockets():
                #     print(
                #         f"[{self.name}] Successfully re-registered after heartbeat timeout."
                #     )
                # else:
                #     print(
                #         f"[{self.name}] Re-registration failed after heartbeat timeout."
                #     )
                _ = self._reset_sockets()  # 嘗試重置連線，無論成功與否都繼續下一輪心跳

            heartbeat_period = time.time()
            while time.time() - heartbeat_period < Gateway.HEARTBEAT_RATE:
                if self._heartbeat_event.is_set():
                    # print(f"[{self.name}] Heartbeat loop is stopping...")
                    break
                time.sleep(0.1)  # 避免忙等

    def stop(self):
        self._heartbeat_event.set()
        if self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()
        try:
            _ = self.registry_sock.send_json(
                {
                    "action": RegistryAction.UNREGISTER.value,
                    "name": self.name,
                    "service_id": self.service_id,
                }
            )
            recv = self.registry_sock.recv_json()
            print(f"[{self.name}] Unregister response: {recv}")
        except zmq.Again:
            print(f"[{self.name}] Unregister timeout. No response from Gateway.")
        self.registry_sock.close()
        self.heartbeat_sock.close()
