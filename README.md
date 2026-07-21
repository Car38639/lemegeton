# lemegeton

人形機器人（humanoid robot）用的輕量分散式通訊中介函式庫（middleware / SDK）。
建立在 **ZeroMQ + Protobuf + Redis** 之上，提供類似 ROS 的通訊模式與服務發現，
讓機器人上各模組（手臂、腰部、移動、手掌、遙操作、感測器…）能以名稱互相溝通，
無需寫死 IP/port。

## 特色

- **服務發現 Gateway**：類似 ROS Master / DNS，服務以名稱註冊、心跳維持存活，
  client 以名稱查詢對方位址；狀態同步到 Redis 以支援跨節點共享與過期清理。
- **三種通訊模式**：
  - Pub / Sub —— 一對多廣播（狀態串流等）
  - Req / Res —— 一問一答（含超時、自動重連）
  - Action —— 仿 ROS Action 的長任務（goal / feedback / result、可取消）
- **共享記憶體**：三重緩衝（triple buffering）無鎖切換，適合影像等高頻大資料。
- **Protobuf 訊息**：以 `lemegeton/msg/` 為單一來源，附帶仿 ROS 的編譯 CLI。

## 安裝

```bash
# 一般安裝（含 protobuf 編譯工具）
pip install ".[build]"
```

安裝後會提供兩個指令：`lemegeton`（專案工具）與 `compile_protos`
（等同 `lemegeton compile`，向後相容）。

> `[build]` extra 會安裝 `grpcio-tools`（內建 protoc 與 `google/protobuf/*`
> well-known types），因此不需要另外手動安裝系統的 `protoc`。

## 訊息（Protobuf）

所有 `.proto` 都放在 **`src/lemegeton/msg/`** 底下，以第一層資料夾當作一個
「訊息 group」：

```
src/lemegeton/msg/
├── common/     # geometry、manipulator、std_msgs
├── humanoid/   # connection、robot_control、state、kinematic
├── teleop/     # teleop、remote_control
├── sensor/     # image
└── template/   # template（範本）
```

### import 慣例

編譯後可直接以套件路徑 import：

```python
from lemegeton.msg.humanoid import robot_control_pb2 as rc
from lemegeton.msg.common import geometry_pb2 as geo

msg = rc.RobotControl(robot_type="humanoid-01")
```

> 關鍵：protoc 生成的 Python import 路徑由 `.proto` **相對於 include root 的路徑**
> 決定。本工具固定把 include root 設在套件的上一層，並要求 `.proto` 內的 import
> 以完整路徑書寫（例如 `import "lemegeton/msg/common/geometry.proto";`），
> 因此生成的 `_pb2.py` 內部一律是 `from lemegeton.msg... import ..._pb2`。

## CLI 用法

`lemegeton` CLI 一律作用於**當前所在的專案**（從當前目錄往上爬尋找
`pyproject.toml` / `CMakeLists.txt`），與工具安裝位置無關。

### `lemegeton create` — 建立 msg/ 骨架

在專案的 package 內建立 `msg/__init__.py` 與 `msg/template/template.proto`：

```bash
cd /path/to/your_project
lemegeton create
```

### `lemegeton compile` — 編譯 protobuf

```bash
lemegeton compile                 # 編譯當前專案 msg/ 下全部 .proto
lemegeton compile -p humanoid     # 只編某個 group
lemegeton compile --proto humanoid/state.proto   # 只編單一檔案
lemegeton compile --list          # 列出發現的 group / proto
lemegeton compile --clean         # 移除生成的 *_pb2.py
lemegeton compile --pkg-dir /other/pkg   # 明確指定其它套件目錄（跨專案）
```

典型流程：

```bash
lemegeton create
lemegeton compile --clean && lemegeton compile
```

> 這套 CLI 是 package-agnostic 的：對任意 `mypkg/msg/**/*.proto`，
> 編譯後皆可 `import mypkg.msg.<group>.<name>_pb2`。

## 快速範例

```python
import zmq
from lemegeton import Context
from lemegeton.client import Publisher, Subscriber
from lemegeton.msg.humanoid import state_pb2

ctx = Context()

# 發布端
pub = Publisher(ctx, name="robot_state", message_class=state_pb2.State)
pub.send(state_pb2.State(...))

# 訂閱端
def on_msg(msg): print(msg)
sub = Subscriber(ctx, name="robot_state", message_class=state_pb2.State, callback=on_msg)
```

更多用法可參考 [test/](test/) 底下的範例（`test_pub`、`test_req`、
`test_action_client/server`、`test_gateway` 等）。

## 部署 Gateway

以 Docker Compose 啟動 Redis 與 Gateway：

```bash
cd deploy
docker compose up -d
```

Gateway 進入點為 [deploy/main.py](deploy/main.py)，預設查詢埠 `60001`。

## 專案結構

```
src/lemegeton/
├── gateway.py         # 服務發現 Gateway + 心跳 client
├── client.py          # Requester / Publisher / Subscriber / ActionClient
├── server.py          # Responder / Publisher / Subscriber / ActionServer
├── serializer.py      # Protobuf 序列化
├── shm_util.py         # 共享記憶體（三重緩衝）
├── project.py         # 專案偵測（walk-up）
├── create.py          # `lemegeton create`
├── compile_protos.py  # `lemegeton compile`（ProtoCompiler）
├── cli.py             # 統一 CLI 進入點
└── msg/               # protobuf 訊息（單一來源）
```
