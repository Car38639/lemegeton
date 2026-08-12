# Lemegeton

以 **ZeroMQ + Protobuf** 為基礎的輕量分散式通訊框架，提供類似 ROS 的 Pub/Sub、Request/Response、Action 三種通訊模式，並透過一個中央 **Gateway**（服務註冊表）讓服務端與客戶端只靠「服務名稱」互相尋址，不需要硬編 IP 與 port。

主要用於人形機器人（humanoid）的遙操作與控制訊息傳遞，內建 teleop、robot control、state、kinematic 等 Protobuf 訊息定義。

---

## 特色

- **名稱尋址**：Server 啟動時自動配置 port 並向 Gateway 註冊；Client 只需要知道服務名稱，Gateway 會回傳實際的 endpoint。
- **自動重連**：Client 以心跳（預設 0.5 秒）持續查詢 Gateway，服務重啟換 port 時會自動重建 socket。
- **TCP / IPC 雙模式**：同機通訊走 abstract IPC socket（低延遲），跨機走 TCP，可設定 `mode="both"` 同時開啟。
- **Protobuf 型別檢查**：收送訊息時檢查 message class，型別不符會警告而不會讓程序崩潰。
- **Action 模式**：支援非同步目標（goal）、進度回報（feedback）、取消（cancel）與 `Future` 結果回收。
- **共享記憶體工具**：`shm_util` 提供三緩衝（triple buffer）的大資料（如影像）零複製傳遞。

---

## 安裝

需求：Python >= 3.9、`protobuf-compiler`（若要自行編譯 `.proto`）。Gateway 不需要任何外部資料庫。

```bash
pip install .
```

相依套件：`protobuf`、`pyzmq`。
（`shm_util` 另外需要 `numpy`，目前未列入 `dependencies`，使用前請自行安裝。）

---

## 架構

```
                 ┌─────────────────────────────────────────┐
                 │                Gateway                  │
                 │  ipc://@lemegeton_registry   註冊 / 註銷 │
                 │  ipc://@lemegeton_heartbeat  心跳       │
                 │  tcp://*:60001               名稱查詢   │
                 │      註冊表 = 行程內的 dict（無外部儲存） │
                 └─────────────────────────────────────────┘
                      ▲                             ▲
       註冊 + 心跳(IPC)│                             │名稱查詢(TCP)
                      │                             │
        ┌─────────────┴──────┐            ┌─────────┴──────────┐
        │  lemegeton.server  │◄──────────►│  lemegeton.client  │
        │     (bind 端)      │  PUB/SUB   │    (connect 端)    │
        └────────────────────┘  REQ/REP   └────────────────────┘
                                ROUTER/DEALER
```

- `lemegeton.server.*`：**bind** 端，會向 Gateway 註冊自己的 endpoint（透過 IPC，因此必須與 Gateway 在同一台機器 / 同一個 IPC namespace）。
- `lemegeton.client.*`：**connect** 端，透過 TCP 向 Gateway（預設 `localhost:60001`）查詢名稱對應的 endpoint。
- Gateway 把服務資訊保存在行程內的 `local_cache`，每 5 秒清理一次；超過 20 秒（2 × 心跳週期 10 秒）沒有心跳的服務會被移除。
- Gateway 重啟後註冊表是空的，但服務的下一次心跳會自動重新註冊（最多 10 秒），不需要外部儲存來持久化。
- 服務名稱具唯一性：同名但 `service_id` 不同的註冊請求會被拒絕（`ALREADY_EXISTS`）。

### 角色配對

| 通訊模式 | Bind 端（`server`） | Connect 端（`client`） | ZMQ Socket |
| --- | --- | --- | --- |
| 發佈 → 訂閱 | `server.Publisher` | `client.Subscriber` | PUB / SUB |
| 訂閱 ← 發佈 | `server.Subscriber` | `client.Publisher` | SUB / PUB |
| 請求 / 回應 | `server.Responder` | `client.Requester` | REP / REQ |
| 動作（Action） | `server.ActionServer` | `client.ActionClient` | ROUTER / DEALER + PUB / SUB |

> 註冊到 Gateway 的名稱是「bind 端」的名稱，client 端要填入**相同的名稱**才找得到對方。
> `server.Broker` 目前尚未實作（`NotImplementedError`）。

---

## 快速開始

### 1. 啟動 Gateway

Gateway 沒有外部相依，直接跑起來即可：

```bash
python3 deploy/main.py
```

或使用 `deploy/` 下的 compose：

```bash
cd deploy
docker compose up -d      # lemegeton-gateway（network_mode: host, ipc: host）
```

### 2. Publisher / Subscriber

```python
import time
import lemegeton
from lemegeton.msg.common.std_msgs_pb2 import String

context = lemegeton.Context()

pub = lemegeton.server.Publisher(
    context=context, name="test_pub", message_class=String, mode="tcp"
)

msg = String(value="Hello, Lemegeton!")
while True:
    pub.send(msg)
    time.sleep(1)
```

```python
import lemegeton
from lemegeton.msg.common.std_msgs_pb2 import String


def message_callback(msg):
    print("Received message:", msg.value)


context = lemegeton.Context()

sub = lemegeton.client.Subscriber(
    context=context,
    name="test_pub",            # 對應 Publisher 註冊的名稱
    message_class=String,
    callback=message_callback,
    ip_address="localhost",     # Gateway 所在位置
    timeout=1.0,
)
```

`Subscriber` 內部使用獨立線程接收，`callback` 會在該線程被呼叫；`CONFLATE` 已開啟，只保留最新一筆訊息。

### 3. Responder / Requester

```python
# Server 端
def message_callback(msg):
    return String(value=f"Responder receive: {msg.value}, hello back!")


responder = lemegeton.server.Responder(
    context=context,
    name="test_responder",
    message_class=String,
    response_class=String,
    callback=message_callback,
    mode="both",
)
```

```python
# Client 端
req = lemegeton.client.Requester(
    context=context,
    name="test_responder",
    message_class=String,
    response_class=String,
    ip_address="localhost",
    timeout=3.0,
)

res = req.send(String(value="Hello, Lemegeton!"))   # 逾時或尚未連線時回傳 None
```

### 4. Action Server / Client

```python
# Server 端：callback 必須回傳 (result_message, success_bool)
def execute_callback(goal_handle) -> tuple[String, bool]:
    for i in range(20):
        time.sleep(1)
        if goal_handle.is_canceled():
            return String(value="Canceled!"), False
        goal_handle.send_feedback(String(value=f"Progress: {i + 1}/20"))
    return String(value="Done!"), True


action_server = lemegeton.server.ActionServer(
    context=context,
    name="test_action_server",
    goal_class=String,
    feedback_class=String,
    result_class=String,
    callback=execute_callback,
    mode="tcp",
)
```

```python
# Client 端
action_client = lemegeton.client.ActionClient(
    context=context,
    name="test_action_server",
    goal_class=String,
    feedback_class=String,
    result_class=String,
    ip_address="192.168.1.100",
    timeout=3.0,
)

goal_id, future = action_client.send_goal(
    String(value="1st goal"), feedback_callback, result_callback, cancel_callback
)

action_client.cancel_goal(goal_id)   # 需要時取消
```

每個 goal 會在 Server 端開一條 worker 線程執行，因此可以同時處理多個 goal；`send_goal` 立即回傳 `(goal_id, Future)`，結果透過 `result_callback(status, result)` 或 `future.result()` 取得。

Action 線路協定（多幀訊息）：

| 方向 | 幀格式 |
| --- | --- |
| Client → Server（目標） | `[b"", b"GOAL", goal_id, payload]` |
| Client → Server（取消） | `[b"", b"CANCEL", goal_id]` |
| Server → Client（結果） | `[routing_id, b"", b"RESULT", goal_id, status, payload]` |
| Server → Client（進度，PUB） | `[goal_id, feedback_payload]` |

`status` 為 `SUCCEEDED` / `FAILED` / `CANCELED`。

---

## 訊息（Protobuf）

內建已編譯的訊息位於 `lemegeton.msg`：

| 模組 | 內容 |
| --- | --- |
| `msg.common.std_msgs` | `Bool` / `String` / `Int` / `Float` / `Double` / `Empty` |
| `msg.common.geometry` | `Vector2/3`、`Quaternion`、`Pose`、`Transform`、`Twist`、`Accel`、`Wrench`、`Inertia`、`Polygon2/3` |
| `msg.common.manipulator` | `JointHeader`、`JointState`、`TrajectoryPoint`、`JointTrajectory` |
| `msg.sensor.image` | `Image`（bytes + shape + dtype + timestamp） |
| `msg.humanoid.robot_control` | `RobotControl`（MC / WBC）、`ModularControl`、`WholeBodyControl`、`HandControl`、`HandGrasp`、`RobotControlSequence` |
| `msg.humanoid.state` | `State`、`StateMessage` |
| `msg.humanoid.connection` | `StateConnection`、`ControlConnection` |
| `msg.humanoid.kinematic` | `ForwardRequest/Respond`、`InverseRequest/Respond` |
| `msg.teleop.teleop` | `TeleopData`（全身關節 Pose）、`TeleopHeader` |
| `msg.teleop.remote_control` | `RemoteControl`、`RemoteControlHeader` |

### 自訂訊息

在專案根目錄建立 `msg/` 資料夾放入 `.proto`（本 repo 的 `msg/` 即為範例來源），然後執行安裝時提供的指令：

```bash
compile_protos
```

該指令（`src/lemegeton/compile_protos.sh`）會遞迴尋找 `msg/**/*.proto`，以 `-I msg` 與 site-packages 路徑編譯，並把 `_pb2.py` 輸出到**已安裝的** `lemegeton/msg/` 底下，同時補上 `__init__.py`。因此自訂訊息可直接以 `from lemegeton.msg.<pkg>.<name>_pb2 import X` 匯入。

若要引用內建訊息，import 路徑請使用套件完整路徑，例如：

```protobuf
import "lemegeton/msg/common/geometry.proto";
```

`src/lemegeton/msg/gen_pb2.sh` 則是用來重新產生**套件內建**訊息的腳本（在 `src/lemegeton/msg/` 下執行 `./gen_pb2.sh -p common/geometry.proto`）。

---

## 共享記憶體（shm_util）

適合傳遞影像等大型資料：`ShmHost` 建立三塊資料緩衝 + 1 byte 控制區，寫入 back buffer 後才切換控制索引；`ShmReader` 依控制索引讀取當前有效緩衝。

```python
from lemegeton.shm_util import ShmHost, ShmReader

host = ShmHost(data_shape=(480, 640, 3), data_type=np.uint8)
meta = host.get_metadata()      # 透過 Protobuf / Gateway 傳給其他行程

reader = ShmReader(meta)        # 另一個行程
frame = reader.get_data()
```

`ShmHost.release()` 會 unlink 共享記憶體；`ShmReader` 預設以 consumer 身分向 `resource_tracker` 取消註冊，避免行程結束時誤刪。

---

## 測試腳本

`test/` 內是手動執行的示範腳本（非 pytest 測試），需先啟動 Gateway：

```bash
python3 test/test_gateway.py         # 或 python3 deploy/main.py
python3 test/test_pub.py             # 搭配 test_sub.py
python3 test/test_responder.py       # 搭配 test_req.py
python3 test/test_action_server.py   # 搭配 test_action_client.py
```

---

## 開發環境（Docker）

根目錄的 `Dockerfile` / `docker-compose.yml` 提供開發用容器（含 `protobuf-compiler`，掛載原始碼、`network_mode: host`、`ipc: host`）：

```bash
docker compose up -d
docker exec -it test_container bash
```

> 由於服務註冊走 abstract IPC socket，容器必須與 Gateway 共用 IPC namespace（`ipc: "host"`）才能註冊成功。

---

## 常見設定

| 項目 | 預設值 | 位置 |
| --- | --- | --- |
| Gateway 查詢埠 | `60001` | `Gateway.DEFAULT_QUERY_PORT` |
| 過期清理週期 | `5` 秒 | `Gateway.run()` |
| 服務心跳週期 | `10` 秒 | `Gateway.HEARTBEAT_RATE` |
| 服務過期 TTL | `20` 秒 | `2 × HEARTBEAT_RATE` |
| Client 查詢週期 | `0.5` 秒 | `CLIENT_HEARTBEAT_INTERVAL` |
| Client 查詢逾時 | `1.0` 秒 | `CLIENT_HEARTBEAT_TIMEOUT` |

---

## 專案結構

```
├── deploy/                  # Gateway 部署（Dockerfile / compose / main.py）
├── msg/                     # 使用者自訂 .proto 來源（由 compile_protos 編譯）
├── src/lemegeton/
│   ├── gateway.py           # Gateway、HeartbeatClient、ServiceType、GatewayStatus
│   ├── server.py            # Publisher / Subscriber / Responder / ActionServer（bind 端）
│   ├── client.py            # Publisher / Subscriber / Requester / ActionClient（connect 端）
│   ├── serializer.py        # Protobuf 序列化封裝
│   ├── shm_util.py          # 共享記憶體三緩衝
│   ├── compile_protos.{py,sh}
│   └── msg/                 # 內建 .proto 與已編譯的 _pb2.py
└── test/                    # 手動示範腳本
```

## Author

Awen Huang <awen_huang@solomon-3d.com>
