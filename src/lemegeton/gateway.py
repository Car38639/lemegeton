import errno
import socket
import threading
import time
import uuid
from enum import Enum
from typing import Optional

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
    # 註冊多久之後才允許對舊擁有者做存活探測。
    # 服務是「先註冊、後 bind」，剛註冊完的那一瞬間端點還沒綁上，
    # 這段緩衝可避免把正在啟動的服務誤判為已死。
    PROBE_GRACE = 3.0

    def __init__(self, port: Optional[int] = None):
        # 註冊表就是這個 dict，沒有外部儲存。
        # 格式: {name: {"data": dict, "service_id": str, "last_seen": float}}
        self.local_cache = {}
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

        self.last_cleanup_time = time.time()

    @staticmethod
    def _address_in_use(family, address, reuse_addr=False) -> Optional[bool]:
        """試綁一個位址。True = 已被佔用，False = 沒人在聽，None = 無法判斷。

        這裡刻意用標準 socket 而不是 ZMQ socket：ZMQ 的 close() 是非同步的，
        探測用的 socket 會短暫留住該 port，剛好撞上服務重啟時配到同一個 port
        就會害它 bind 失敗。標準 socket 的 close() 是同步的，不留殘影。
        """
        probe = socket.socket(family, socket.SOCK_STREAM)
        try:
            if reuse_addr:
                # 允許跨過 TIME_WAIT，但若真的有 listener 仍會是 EADDRINUSE
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(address)
            return False
        except OSError as e:
            return True if e.errno == errno.EADDRINUSE else None
        finally:
            probe.close()

    def _owner_is_alive(self, info) -> bool:
        """探測舊註冊的擁有者是否還活著。

        作法是試著去綁它註冊的位址：綁得起來就代表沒有任何行程在監聽，
        該服務已經崩潰；綁不起來（EADDRINUSE）代表還有人佔著，仍然活著。
        這樣崩潰的服務不必等滿 TTL 才能被同名重啟接管。
        """
        endpoint = (info.get("data") or {}).get("endpoint") or {}
        results = []

        ipc_path = endpoint.get("ipc")
        if ipc_path:
            if ipc_path.startswith("@"):
                # ZMQ 的 ipc://@name 對應 Linux abstract socket，位址是 "\0name"
                results.append(
                    self._address_in_use(socket.AF_UNIX, "\0" + ipc_path[1:])
                )
            else:
                results.append(None)  # 檔案系統路徑的 ipc，無法用試綁判斷

        tcp_port = endpoint.get("tcp")
        if tcp_port:
            results.append(
                self._address_in_use(
                    socket.AF_INET, ("", int(tcp_port)), reuse_addr=True
                )
            )

        if any(r is True for r in results):
            return True  # 任一端點還被佔著 → 還活著
        if any(r is False for r in results):
            return False  # 有明確結論且都沒人在聽 → 已死
        return True  # 沒有端點資訊或無法判斷，保守視為還活著

    def _can_take_over(self, name, current_service, request_service_id):
        """判斷同名的舊註冊是否已失效，可讓新的 service_id 接管"""
        age = time.time() - current_service.get("last_seen", 0)
        if age > self.ttl:
            reason = "心跳已超過 TTL"
        elif age > Gateway.PROBE_GRACE and not self._owner_is_alive(current_service):
            reason = "註冊的端點已無人監聽"
        else:
            return False

        print(
            f"[Registry] 服務 {name} 的舊註冊已失效（{reason}），"
            f"允許 {request_service_id} 接管"
        )
        return True

    def cleanup_stale(self):
        """清理心跳已過期的服務"""
        now = time.time()
        stale_keys = [
            name
            for name, info in self.local_cache.items()
            if now - info.get("last_seen", 0) > self.ttl
        ]

        for name in stale_keys:
            del self.local_cache[name]
            print(f"[Cleanup] 服務 {name} 已過期，執行清理")

        self.last_cleanup_time = now

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
                        # 心跳斷了超過 TTL 就視同過期,或是註冊的端點已經沒人監聽
                        # （代表行程已死），兩者都直接讓新身份接管。
                        if (
                            current_service
                            and current_service.get("service_id") != request_service_id
                            and self._can_take_over(
                                name, current_service, request_service_id
                            )
                        ):
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
                        # 註冊表不認得這個名稱（例如 gateway 剛重啟過），
                        # 直接把這次心跳當成重新註冊，讓服務自行癒合。
                        print(
                            f"[Heartbeat] 服務 '{name}' 不在註冊表中，以本次心跳重新註冊，ID: {new_service_id}"
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

                # 每 5 秒清理一次心跳過期的服務
                if time.time() - self.last_cleanup_time > 5:
                    self.cleanup_stale()

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
        # 心跳期間發現名稱被別的 service_id 佔走時會設為 True，供服務端查詢
        self.name_conflict = False
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

                # Gateway 在心跳路徑回 MISMATCH、在註冊路徑回 ALREADY_EXISTS，
                # 兩者都代表「這個名稱現在不屬於我」。原本只檢查 ALREADY_EXISTS，
                # 因此心跳路徑的衝突永遠偵測不到，兩個同名服務會互相覆寫註冊資訊。
                if resp.get("status") in (
                    GatewayStatus.MISMATCH.value,
                    GatewayStatus.ALREADY_EXISTS.value,
                ):
                    print(
                        f"[{self.name}] 服務名稱已被其他實例佔用（service_id 不符），"
                        f"停止心跳以免覆寫對方的註冊。本服務將無法再被查詢到。"
                    )
                    self.name_conflict = True
                    self._heartbeat_event.set()  # 停止心跳，避免干擾他人
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
        # 冪等：重複呼叫時第二次的 send_json 會撞到已關閉的 socket
        if getattr(self, "_stopped", False):
            return
        self._stopped = True

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
