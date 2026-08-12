# 大型 payload 傳遞：以 blob 取代 shm_util

**狀態**：設計提案，尚未實作進 repo（原型已完成並通過驗證，見文末）
**日期**：2026-08-13

---

## 1. 問題

`shm_util` 提供共享記憶體傳遞影像，但目前是**未接線的死碼**：

| 事實 | 影響 |
| --- | --- |
| 沒有任何程式碼引用（`__init__.py` 只匯出 `client`/`server`/`gateway`） | 移除不影響任何現有功能 |
| `numpy` 不在 `dependencies` 裡 | 乾淨安裝後 `import lemegeton.shm_util` 直接 `ImportError`，**目前無法使用** |
| 沒有任何 `.proto` 定義 shm metadata | `get_metadata()` 的 dict 得使用者自己想辦法傳給對端，握手完全沒做 |
| README 宣稱「零複製」 | **不正確**：`set_data` 一次 `np.copyto`、`get_data` 又一次，實際兩次複製 |

同時，走 protobuf 的影像傳輸很慢。問題出在把數 MB 的像素塞進 `bytes` 欄位。

---

## 2. 量測環境

- 容器 `python:3.10-slim` + `pyzmq 27.1.0` / `libzmq 4.3.5` / `protobuf 7.35.1`（後端為 **upb**，已是 C 實作，沒有「換實作就變快」的空間）
- ZMQ 傳輸一律走 `ipc://`（abstract socket），`--ipc host`
- 每組取 40~60 次，去掉最快 10% 的暖機樣本後取平均

---

## 3. 測試結果

### 3.1 成本拆解：貴的是 payload，不是 schema

同一個 `Image` 訊息，`data` 欄位留空只放 metadata 時：

| | 序列化 | 解析 | 大小 |
| --- | --- | --- | --- |
| 只有 metadata（shape/dtype） | **0.6µs** | **0.3µs** | 14 bytes |
| 內含 1080p 像素 | 334µs | 96µs | 6.2MB |

**schema 驗證與 varint 編碼的成本等於零。** 貴的是 `tobytes()` → 指派 `bytes` 欄位 → `SerializeToString()` 這三次數 MB 的配置與複製。

### 3.2 傳輸方式比較（送出端耗時）

| 解析度 | 大小 | ZMQ ipc + protobuf | ZMQ ipc 直送 raw | 共享記憶體 |
| --- | --- | --- | --- | --- |
| VGA 640×480 | 0.9MB | 0.29ms | 0.11ms | 0.04ms |
| 720p | 2.8MB | 1.05ms | 0.37ms | 0.12ms |
| 1080p | 6.2MB | 2.86ms | 1.03ms | 0.35ms |
| 4K | 24.9MB | 17.81ms | 5.48ms | 2.46ms |

### 3.3 關鍵發現：把 protobuf 的 bytes 包裝拿掉後，shm 的優勢消失

| | 1080p | 佔 30fps 預算 | 4K | 佔 30fps 預算 |
| --- | --- | --- | --- | --- |
| protobuf 內含像素 | 3.25ms | 9.8% | 15.21ms | 45.6% |
| 單段 raw bytes | 0.96ms | 2.9% | 5.20ms | 15.6% |
| 單段 zero-copy | 0.54ms | 1.6% | **2.67ms** | 8.0% |
| 共享記憶體 | 0.36ms | 1.1% | **2.64ms** | 7.9% |

**4K 下兩者只差 0.03ms，1080p 差 0.19ms。** 原因是 `shm_util` 並非零複製（兩次 `np.copyto`），而 ZMQ 的 `copy=False` 在送出端是真正零複製，兩者收斂。

### 3.4 多消費者擴展性（1080p，生產者 `send()` 耗時）

| 消費者數 | ZMQ ipc PUB | 共享記憶體 |
| --- | --- | --- |
| 1 / 2 / 4 / 8 | 0.62 / 0.40 / 0.37 / 0.22 ms | 0.24 / 0.24 / 0.23 / 0.27 ms |

PUB 的送出成本**不隨消費者數上升** —— ZMQ 是非同步的，複製由背景 I/O 執行緒處理，不在生產者的關鍵路徑上。
（該複製仍消耗 CPU，只是不阻塞呼叫端；本次測試的訂閱者與生產者同行程，無法乾淨切分那部分成本，**未量測**。）

### 3.5 CONFLATE 與 multipart 不相容 —— 決定了設計走向

框架的 `Subscriber` 靠 `CONFLATE=1` 只取最新影格，因此「影像改走 multipart」這條路**不可行**：

| 測試 | 送出 | 收到 |
| --- | --- | --- |
| 單段 + `CONFLATE=1`（對照） | 20 則 | 1 則、1 frame（`seq19`）✓ |
| 單段，無 CONFLATE（對照） | 20 則 | 20 則、每則 1 frame ✓ |
| **multipart + `CONFLATE=1`** | 20 則 × 3 frame | **1 則、只有 1 個 frame（`tail19`）** |
| multipart，無 CONFLATE（對照） | 20 則 × 3 frame | 20 則、每則 3 frame ✓ |

CONFLATE 把每個 frame 當成獨立訊息各自 conflate，multipart 的分組被摧毀。**失效模式不是堵塞，是靜默的資料破壞** —— 收端拿到殘缺訊息，`frames[0]` / `frames[1]` 那種寫法會讀到錯的內容或 `IndexError`。ZMQ 官方文件亦載明 `ZMQ_CONFLATE` 不支援多段訊息。

### 3.6 HWM 與 multipart 相容（另一個常見誤解）

`SNDHWM=5`，接收端完全不讀：

| 每則 frame 數 | 1 | 2 | 3 | 10 |
| --- | --- | --- | --- | --- |
| 塞進的則數 | 5 | 5 | 5 | 5 |
| frame 總數 | 5 | 10 | 15 | 50 |

**HWM 以「訊息則數」計算且確實生效**，不會無限堆積；PUB/SUB 的丟棄行為也正常（送 50 則收到 2 則，每則 3 frame 完整）。唯一要留意的是 HWM 管則數不管位元組，10 frame/則會佔 10 倍記憶體 —— 但這對單段的大訊息同樣成立，非 multipart 特有。

### 3.7 巢狀結構化資料不需要這個機制

| 訊息 | 大小 | 序列化 | 解析 | 1000Hz 時的 CPU |
| --- | --- | --- | --- | --- |
| `TeleopData`（23 Pose + 50 dexterous Pose） | 2.2KB | 3.6µs | 2.5µs | 0.49% |
| `RobotControl`（ModularControl + 7 DOF） | 93B | 0.3µs | 0.4µs | — |

**`teleop.proto` 這類不要碰。** 順帶一提，從零組裝 `TeleopData`（47.2µs）比序列化貴 13 倍 —— 真要優化 teleop，該動的是 Python 端的欄位賦值，不是傳輸層。

---

## 4. 設計

### 4.1 原則

**schema 照舊由 protobuf 管，只把不透明的大型 payload 挪出訊息體。** 使用者的 `.proto` 一個字都不用改 —— `Image` 保留它的 `bytes data`，小圖與壓縮影像仍可照舊放在欄位裡。

### 4.2 新增的 proto

```protobuf
// lemegeton/msg/common/blob.proto
syntax = "proto3";
package lemegeton.msg.common.blob;

message BlobSpec {
  string field = 1;          // 欄位路徑，例如 "data" 或 "rgb.data"
  repeated int32 shape = 2;  // ndarray 形狀；純 bytes 時留空
  string dtype = 3;          // "uint8" / "float32" ...
  uint64 offset = 4;         // 在 payload 區的起始位移
  uint64 size = 5;           // 位元組長度
}

message BlobEnvelope {
  string type_url = 1;       // header 的完整型別名，收端據此驗證
  bytes header = 2;          // 序列化後的使用者訊息（已剝除大欄位）
  repeated BlobSpec blobs = 3;
}
```

### 4.3 線路格式

```
[b"LBLB"][uint32 信封長度][BlobEnvelope][blob0][blob1]...
```

- **單一 ZMQ frame** → `CONFLATE` 照常運作（見 3.5）
- 開頭的 magic 讓收端能分辨 blob 封包與一般訊息，**同一個 topic 可以混送**，舊版收端也不會誤判
- `type_url` 提供型別驗證：型別不符時 `unpack` 直接拋 `TypeError`

### 4.4 API

```python
# 送出端
publisher.send(message, blobs={"data": frame})

# 接收端（Subscriber 建構時 unpack_blobs=True）
def on_frame(message, blobs):
    frame = blobs["data"]        # ndarray view，零複製
```

框架端需要的改動（都是加法，不影響既有行為）：

| 檔案 | 內容 |
| --- | --- |
| `msg/common/blob.proto` + `blob_pb2.py` | 上述兩個訊息 |
| `blob.py` | `pack` / `unpack` / `is_blob`；`numpy` 為延遲匯入，只傳 bytes 時不需要 |
| `serializer.py` | `decode_payload()`，統一 blob 與一般訊息的解碼 |
| `server.py` / `client.py` | `Publisher.send(message, blobs=None)`、`Subscriber(unpack_blobs=False)` |
| `pyproject.toml` | optional extra `array = ["numpy"]` |

### 4.5 巢狀資料怎麼處理

靠**欄位路徑**：`pack` 走到擁有該欄位的子訊息，`ClearField` 掉它（避免被序列化兩次），把 shape/dtype/位移記進 `BlobSpec`。

```python
def _resolve(message, path):
    """把 "rgb.data" 拆成 (擁有該欄位的訊息, 欄位名)"""
    parts = path.split(".")
    owner = message
    for name in parts[:-1]:
        owner = getattr(owner, name)
    return owner, parts[-1]
```

於是一個同時含大 payload 與結構化資料的訊息可以這樣處理：

```python
# message SensorBundle { string robot_id; Image rgb; Image depth; TeleopData teleop; }
payload = blob.pack(bundle, {"rgb.data": rgb, "depth.data": depth})
# → rgb/depth 的像素移出訊息體；robot_id、shape/dtype、整個 teleop 子訊息都留在 header
```

---

## 5. 原型驗證結果

### 5.1 巢狀情境（`SensorBundle` = 1080p RGB + 1080p float32 depth + `TeleopData`，共 14.4MB）

| 項目 | 結果 |
| --- | --- |
| 巢狀結構化資料（`TeleopData`，25 個 dexterous pose）原樣保留 | ✅ |
| 巢狀訊息的 metadata（shape/dtype/robot_id）保留 | ✅ |
| 大欄位確實被移出訊息體 | ✅ |
| rgb / depth 內容逐位元組一致（含 float32） | ✅ |
| 解出的是零複製 view | ✅ |
| 型別不符時擋下（`demo.SensorBundle` vs `Image`） | ✅ |
| 信封額外開銷 | 629 bytes |
| **端到端：protobuf 內含像素 10.87ms → blob 2.89ms** | **3.8 倍** |

### 5.2 真實 OpenCV 影像（OpenCV 5.0）

| 項目 | 結果 |
| --- | --- |
| `cv2.imread` 原圖 720p 逐點一致 | ✅ |
| **裁切後的非連續記憶體**（`img[100:600, 200:1000]`）逐點一致 | ✅ |
| `cv2.flip` 結果逐點一致 | ✅ |
| shape/dtype 由 protobuf 正確帶回 | ✅ |
| 還原後可直接餵回 cv2（`imwrite`/`imread` 往返一致） | ✅ |
| 同一 topic 混送一般訊息不會壞掉 | ✅ |

非連續記憶體是最容易踩的坑 —— OpenCV 做過 crop 或 ROI 之後 stride 不連續，直接送會拿到錯的像素排列，`np.ascontiguousarray` 可擋掉。

### 5.3 參考用法

```python
def publish_frame(publisher, capture) -> bool:
    """從 cv2.VideoCapture 讀一幀並送出"""
    ok, frame = capture.read()          # (H, W, 3) uint8，OpenCV 是 BGR
    if not ok:
        return False

    # 裁切/transpose 過的 frame 不是連續記憶體，統一保證，否則 pack() 會多一次複製
    frame = np.ascontiguousarray(frame)

    message = Image(shape=list(frame.shape), dtype=frame.dtype.name)
    message.timestamp.GetCurrentTime()

    publisher.send(message, blobs={"data": frame})   # 像素不進 message.data
    return True


def on_frame(message, blobs):
    """Subscriber callback：還原成可直接使用的 numpy 陣列"""
    frame = blobs.get("data")

    if frame is None:                   # 同一 topic 也可能收到沒走 blob 的一般訊息
        if not message.data:
            return None
        frame = np.frombuffer(message.data, dtype=np.dtype(message.dtype)).reshape(
            tuple(message.shape))

    # ⚠️ 走 blob 時 frame 是接收緩衝的 view，只在這個 callback 內有效。
    #    要保留請自行 frame.copy()
    return frame
```

---

## 6. 取捨與注意事項

1. **`unpack` 回傳的是 view**，只在該接收緩衝存活期間有效。使用者若要保留資料或丟給別的執行緒，必須 `.copy()`。這是本設計最容易出事的地方，必須寫進文件。
2. **`pack` 每次回傳全新 bytearray**，因此 `send(copy=False)` 是安全的 —— 不像直接送相機 buffer 那樣有撕裂風險。
3. **不要一律走 blob**。JPEG 壓過的 frame 約 100KB，protobuf 成本可忽略，直接放 `message.data` 即可。建議以大小為門檻（例如 >1MB）。
4. **若能接受壓縮**，JPEG 會讓資料量降一個數量級，上述所有優化都變成雜訊 —— 代價是編解碼 CPU 與失真。屬於另一層次的取捨。
5. 更快的 multipart 方案（1080p 0.40ms、4K 3.30ms，與 shm 同級）**需要自行實作 conflate**（小 `RCVHWM` + 排空到最新一則），且 `copy=False` 直送相機 buffer 有生命週期陷阱。除非影像路徑被證實是瓶頸，否則不建議。

---

## 7. 結論

**建議移除 `shm_util`，以 blob 取代。**

- shm 能提供的效益，用「protobuf 只放 header」就能拿到絕大部分：4K 差 0.03ms、1080p 差 0.19ms
- blob 保留了 protobuf schema、gateway 名稱尋址與整套既有基礎設施；shm 則需要另一套 metadata 握手、生命週期管理與撕裂防護
- 移除 `shm_util` 的實質影響接近零：沒有任何程式碼用它，且因缺 numpy 相依，現在也不可能有人在用
- 現有實作若要真的能用，得補齊 metadata 的 proto 與握手、寫入序號防撕裂、生產者死亡時的清理、不依賴 CPython 私有 API —— 工作量接近重寫

### 建議落地順序

1. 實作 `blob.proto` + `blob.py` + `Publisher.send(blobs=)` / `Subscriber(unpack_blobs=)`
2. 影像發布端改用 blob，量測實際的相機解析度與幀率確認夠用
3. 確認後移除 `shm_util`，並清掉 TODO 中相關的三個項目
4. 把「CONFLATE 與 multipart 不相容」寫進 README 注意事項
