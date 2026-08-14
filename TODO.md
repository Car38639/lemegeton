# TODO — Bug 與風險清單

以靜態閱讀 `src/lemegeton/`、`deploy/`、`test/` 與建置設定整理而成（2026-08-12）。
標註 `⚠️` 者為會直接造成當機、掛死或行為錯誤的問題。未特別註明者皆為靜態分析結論，尚未以執行環境驗證（本機未安裝 `pyzmq` / `protobuf`）。

| 分類 | 數量 | 已修正 |
| --- | --- | --- |
| P0 正確性錯誤 | 10 | 10 |
| P1 穩健性 / 資源管理 | 19 | 8 |
| P2 專案 / 建置 / 工具 | 11 | 0 |
| 安全性與部署風險 | 7 | 1 |
| 部署與版本落差 | 2 | 2 |

> 已修正項目標記為 ✅。驗證分兩層：
> 1. **stub 單元驗證**：以 stub 取代 `pyzmq`，直接驅動 `Responder._response_process`、
>    `Gateway.run()` 查詢分支、`ActionFeedbackSubscriber._subscribe_process`。
> 2. **端到端驗證**：對接執行中的 `lemegeton-gateway`，先以掛載原始碼與舊版做對照，
>    重建映像後再以**部署版本本身**（`site-packages`，未掛 `PYTHONPATH`）複測，四項全過。
>
> 部署映像原本比 repo 多兩處未進版控的修改，已回填至 [gateway.py](src/lemegeton/gateway.py)；
> 重建後的映像已同時包含回填內容與本輪修正（見下方「部署與版本落差」）。

---

## P0 — 正確性錯誤（會當機、掛死或送出錯誤資料）

- [x] ✅ ⚠️ **Client 在尚未取得 endpoint 時就 `connect(None)`，建構子直接失敗**
  [client.py](src/lemegeton/client.py)
  `Subscriber` / `ActionFeedbackSubscriber` / `ActionClient` 只 `time.sleep(0.5~1.0)` 就呼叫 `_init_socket()`，其中 `self._socket.connect(self._endpoint)` 在 Gateway 未啟動、服務尚未註冊或網路稍慢時 `self._endpoint` 仍是 `None` → 例外（實測舊版：`TypeError: expected bytes, NoneType found`）。
  **已修正**：`ClientCore` 新增 `_endpoint_ready` 事件與 `wait_for_endpoint(timeout)`，建構子改為「等待」而非盲目 sleep（新增 `connect_timeout` 參數，預設 5 秒）；所有 `_init_socket()` 改為回傳 bool，endpoint 未就緒時不建 socket，收送迴圈會持續等服務上線後自動連上。等不到也不再讓建構子失敗。

- [x] ✅ ⚠️ **`ActionClient` 結果幀長度檢查 off-by-one，導致結果線程無聲死亡**
  [client.py](src/lemegeton/client.py)
  收到的幀是 `[b"", b"RESULT", goal_id, status, payload]`（5 幀），但檢查為 `if len(frames) < 4: continue`，之後卻存取 `frames[4]`。剛好 4 幀時 `IndexError`，而該迴圈只捕捉 `zmq.ZMQError` → 線程直接結束，`Future` 永遠不會完成。
  **已修正**：改為 `< 5`，並把 `except zmq.ZMQError: break` 換成 ContextTerminated / ZMQError / Exception 三段處理，單筆訊息處理失敗不再讓整條 listener 消失。

- [x] ✅ ⚠️ **`Responder` 反序列化失敗時回傳錯誤型別**
  [server.py:97-150](src/lemegeton/server.py#L97-L150)
  回覆用 `self._message_class()` 建構，應為 `self._response_class()`。當請求與回應型別不同時，Client 會把 request 的空訊息當成 response 解析。
  **已修正**：統一改用 `self._response_class()`。

- [x] ✅ ⚠️ **`Responder` callback 拋例外時不回覆，REP 狀態機卡死**
  [server.py:97-150](src/lemegeton/server.py#L97-L150)
  callback 例外被外層 `except` 吃掉後直接進下一輪 `recv()`，但 REP socket 必須「收一次、送一次」，於是後續每次 `recv` 都會拋 `EFSM`，該服務等同永久失效；Client 端只會看到逾時。
  **已修正**：`_response_process` 拆成「收 / 處理 / 送」三段，只要成功 `recv` 就一定會送出回應；callback 例外、型別錯誤、反序列化失敗一律回傳空的 `response_class`。

- [x] ✅ ⚠️ **`Responder` 的 `except zmq.ContextTerminated` 是無效程式碼**
  [server.py:97-150](src/lemegeton/server.py#L97-L150)
  前面已有 `except Exception as e`，後面兩個 handler 永遠不會被執行。Context 被 term 之後迴圈不會 break，而是持續 poll 失敗並瘋狂列印錯誤。
  **已修正**：`zmq.ContextTerminated` 移到 `zmq.ZMQError` 之前，收 / 送兩段各自處理；業務邏輯的例外不再與 socket 例外混在同一個 `try`。

- [x] ✅ ⚠️ **Gateway 從 Redis 回填查詢結果時多包了一層 dict，導致服務永遠找不到**
  [gateway.py:221-248](src/lemegeton/gateway.py#L221-L248)
  本地快取命中時回傳 `res_info["data"]`，但 Redis 命中時回傳整包 `{"data": ..., "service_id": ..., "last_seen": ...}`。Client 端讀 `resp["data"]["type"]` 會取不到 → 一律判定型別不符；同時 `local_cache[name] = {"data": data}` 又把結構再包一層，污染後續查詢。
  **已修正**：改為先回填快取（保持與 `local_cache` 相同層級）再統一從 `res_info["data"]` 取值，兩條路徑共用同一段回覆邏輯；缺 `data` 欄位時回 `NOT_FOUND` 而非 `KeyError` 讓 Gateway 崩潰。

- [x] ✅ ⚠️ **`ActionServer` 在 `mode="ipc"` 時完全沒有 bind**
  [server.py:377-387](src/lemegeton/server.py#L377-L387)
  只處理 `self._enable_tcp`，但 `ServiceCore` 仍會把 ipc endpoint 註冊到 Gateway，Client 於是連到一個不存在的位址（且 `localhost` 情況下 Client 會優先選 ipc，見 [client.py:47-52](src/lemegeton/client.py#L47-L52)）。
  **實測後發現影響比原本記錄的更廣**：`mode="both"` 同樣失效 —— ROUTER 只綁了 tcp，但 localhost 的 client 一律優先挑 ipc，於是照樣連到不存在的位址。而且 `send_goal` **完全不報錯**，只是結果永遠不會回來（實測 `TimeoutError`），屬於最難查的靜默失效。
  **已修正**：ROUTER socket 補上 ipc bind，與 `Responder` / `Publisher` / `Subscriber` 的作法一致（feedback Publisher 本來就會依 `mode` 自行雙綁，不需另外處理）。

- [x] ✅ ⚠️ **`ActionClient` 的 DEALER socket 被兩個執行緒同時使用**
  [client.py](src/lemegeton/client.py)
  `send_goal` / `cancel_goal` 在使用者執行緒送，`_result_listener_loop` 在背景執行緒收。ZMQ socket 非執行緒安全，會出現偶發的當機或幀錯亂。Server 端已用 `_socket_lock` 保護（[server.py:362](src/lemegeton/server.py#L362)），Client 端漏了。
  **已修正**：新增 `_dealer_lock`（RLock），`send_goal` / `cancel_goal` / listener 的 poll+recv 全部序列化；listener 改用 20ms 短 poll 並在鎖外讓出，避免餓死送出端。DEALER 也改為由 `_ensure_dealer()` 延遲建立/重建，endpoint 未就緒時 `send_goal` 拋 `ConnectionError` 而不是操作 `None`。
  順帶修掉一個潛在死鎖：`future.set_result()` 原本在 `goals_lock` 內呼叫，而它會同步觸發使用者的 `result_callback`，callback 裡若再 `send_goal` 就會卡死；現改為在鎖外設值。

- [x] ✅ ⚠️ **心跳的名稱衝突偵測失效（狀態碼對不上）**
  Gateway 心跳回應的是 `MISMATCH`（[gateway.py:184-213](src/lemegeton/gateway.py#L184-L213)），但 `HeartbeatClient` 只檢查 `ALREADY_EXISTS`。結果：同名服務被搶註後仍持續心跳、不會自我停止，兩個服務互相覆寫註冊資訊（實測舊版：冒名者心跳線程持續存活）。
  **已修正**：改為同時接受 `MISMATCH` 與 `ALREADY_EXISTS`（前者是心跳路徑、後者是註冊路徑，語意都是「這個名稱不屬於我」）。偵測到之後停止心跳、設定 `HeartbeatClient.name_conflict`，並印出明確警告（原本那行 print 是被註解掉的，就算偵測成功也沒人會知道）。服務端可透過 `ServiceCore.name_conflict` 查詢自己是否已被搶名。

- [x] ✅ ⚠️ **Action feedback 閒置超時會讓整個 ActionClient 失效**
  [client.py:463-482](src/lemegeton/client.py#L463-L482)、[client.py:635-639](src/lemegeton/client.py#L635-L639)
  `ActionFeedbackSubscriber` 只要 `timeout`（預設 30 秒）內沒收到任何 feedback 就 `break` 並設定 timeout flag，`_result_listener_loop` 看到 flag 也跟著結束。對「長時間沒有任務」的正常閒置情境，client 會在 30 秒後自己壞掉，之後送 goal 不再有任何結果。
  **已修正**：`zmq.Again` 視為閒置改為 `continue`；`_timeout_flag` 改在「非主動關閉卻離開迴圈」時才設定，保留原本「feedback 通道真的掛掉就一併收掉 result listener」的用意，同時不再誤判閒置。

---

## P1 — 穩健性、資源管理與併發

- [x] ✅ **Client 心跳線程遇到任意例外就永久終止**
  [client.py](src/lemegeton/client.py)：`except Exception: break` → 之後再也不會重新查詢 endpoint。
  **根因比表面更嚴重**：心跳的 REQ socket 沒有設 `REQ_RELAXED`，Gateway 一逾時就卡在「等待回覆」狀態，下一輪 `send` 直接 `EFSM` → 落入 broad except → 線程死亡。實測舊版把 `query_port` 指向沒人在聽的埠，**2 秒**就出現 `Heartbeat error: Operation cannot be accomplished in current state` 並終止。
  **已修正**：心跳 socket 補上 `REQ_RELAXED` / `REQ_CORRELATE`；`zmq.Again` 視為 gateway 暫時不可達（保留最後已知 endpoint 繼續運作）；其他例外改為重建 socket 後重試，只有 `ContextTerminated` 才結束線程。另把 `time.sleep` 換成 `_heartbeat_stop_event.wait()`，`close()` 不必再等滿一個心跳週期。

- [x] ✅ **`ActionClient.close()` 沒有呼叫 `super().close()`**
  [client.py](src/lemegeton/client.py)：心跳線程與心跳 socket 洩漏；且在 `_init_sockets()` 之前呼叫 `close()` 會因 `self._stop_event` 未建立而 `AttributeError`。
  **已修正**：`close()` 補上 `super().close()`；`_stop_event` / `dealer_socket` / `_dealer_lock` 改在 `__init__` 最前段就建立。

- [x] ✅ **Gateway 對 Redis 完全沒有錯誤處理**
  Redis 斷線或逾時會讓 `run()` 整個拋出，Gateway 直接下線 → 所有服務失去尋址能力。一個為了提升韌性而加的元件，反而成為單點故障。
  **已修正 —— 直接移除 Redis**。分析後確認它的唯一作用是「縮短 gateway 重啟後的空窗」，而沒有它時服務的下一次心跳（≤10 秒）就會自動重新註冊，空窗本來就有界。三個佐證：
  1. 查詢路徑的 Redis 回填長期壞著（見上方 P0 的多包一層 bug），實際行為等同沒有 Redis，而沒有人發現。
  2. REGISTER 分支從不讀 Redis，所以它連「跨重啟的名稱唯一性」都沒有保障。
  3. 跨主機共用註冊表做不到 —— 註冊與心跳走 abstract IPC socket（作用域是 network namespace），服務只能向同機的 gateway 註冊。
  註冊表現在就是行程內的 dict，`sync_and_cleanup` 改名為 `cleanup_stale` 且只做過期清理，`Gateway.__init__` 移除 `redis_conf` 參數，套件相依與 `deploy/docker-compose.yml` 的 redis service 一併拿掉。
  **實測（`--network none` 的完全隔離容器，自建 gateway）**：殺掉 gateway 再重啟，服務**6.5 秒**就靠心跳自行重新註冊完成（上限是一個心跳週期 10 秒），停機期間既有的 PUB/SUB 與 REQ/REP 連線完全不受影響。
  附帶收穫：Gateway 沒有外部相依之後，整套系統可以在隔離的 network namespace 裡跑完整測試，不必再碰線上的 gateway。

- [ ] **Gateway 對非法輸入沒有防護**
  三個 REP socket 都直接 `recv_json()`，收到非 JSON 或缺欄位的訊息就拋例外並終止服務；`GatewayStatus.INVALID_FORMAT`（[gateway.py:27](src/lemegeton/gateway.py#L27)）定義了卻從未使用。

- [ ] **Gateway 是單執行緒 + 阻塞式 REP**
  任一分支阻塞就會拖住註冊、心跳與查詢；REP 模式下若某個 client 送了請求卻不收回應，該 socket 亦會卡在 send 狀態。

- [x] ✅ **服務崩潰後 20 秒內無法以同名重啟**
  [gateway.py](src/lemegeton/gateway.py) + [server.py:45-48](src/lemegeton/server.py#L45-L48)：舊紀錄要等 TTL（2 × 10 秒）過期才會清掉，期間新程序註冊被回 `ALREADY_EXISTS`，`ServiceCore` 直接 `raise` → 服務起不來。
  **分兩階段修正**：
  1. TTL 接管 + 註冊重試（原本只存在於部署映像，已回填進版控）：`last_seen` 超過 TTL 的舊條目允許新 `service_id` 接管，`HeartbeatClient` 註冊失敗重試至 TTL+5 秒才放棄。**但實測仍要等 18.0 秒**（TTL 20 秒扣掉最後一次心跳的時間差），而重試上限是 25 秒，只剩 7 秒餘裕。
  2. 主動存活探測（本次）：新增 `Gateway._owner_is_alive()`，試著去綁舊註冊的端點 —— 綁得起來代表沒人在監聽、行程已死，可**立刻**接管，不必等滿 TTL。純 gateway 端改動，不動通訊協定，對舊版 client 完全相容（相對之下，調快心跳頻率會讓還沒升級的服務被誤判過期）。
  探測刻意用**標準 socket 而非 ZMQ socket**：ZMQ 的 `close()` 是非同步的，探測用的 socket 會短暫留住那個 port，若重啟的服務剛好被 `_allocate_port()` 配到同一個 port 就會 bind 失敗（測試中實際撞到過）。標準 socket 的 `close()` 是同步的，不留殘影；TCP 探測加 `SO_REUSEADDR` 以跨過 TIME_WAIT，但真的有 listener 時仍會是 `EADDRINUSE`。
  服務是「先註冊、後 bind」，剛註冊完的瞬間端點還沒綁上，因此加了 `PROBE_GRACE = 3.0` 秒緩衝，避免把正在啟動的服務誤判為已死；同一個 `service_id` 重送註冊則完全不觸發探測。
  **部署版實測：接管耗時 18.0s → 0.0s**，且服務還活著時同名註冊仍被正確拒絕（`ALREADY_EXISTS`）。

- [x] ✅ **Action 的最後一筆 feedback 會被 RESULT 搶先而遺失**（端到端實測發現）
  RESULT 走 DEALER、feedback 走 SUB，是兩條獨立通道。Server 送完最後一筆 feedback 後緊接著送 RESULT，Client 的 result listener 先收到 RESULT 就呼叫 `untrack_goal()`，稍後才抵達的 feedback 因為已不在 `active_goals` 而被丟棄。
  **實際影響比原本記錄的嚴重得多**。原先只在快速情境測到「9 筆掉 1 筆」，但只要使用者的 feedback callback 稍慢（每筆 40ms），RESULT 會在**第一個 callback 還在執行時**就抵達並解除追蹤 —— 實測舊版每個任務 3 筆只收到 **1 筆（6/18）**。
  **已修正**：Server 為每個 goal 的 feedback 編流水號，並在 RESULT 幀附上總筆數；Client 收到 RESULT 後，先等 `received_seq` 追上總數（上限 `feedback_grace`，預設 0.3 秒）才解除追蹤並完成 Future。計數在**使用者 callback 執行完之後**才遞增，因此「收齊」等於「所有進度都已送到使用者手上」，也保證 feedback 不會出現在 `result_callback` 之後。實測 18/18，順序全部正確。
  **線路格式相容**：feedback 由 `[goal_id, payload]` 變成 `[goal_id, payload, seq]`，RESULT 由 6 幀變成 7 幀，新欄位都**附加在尾端**，新舊混用皆已實測通過（新 server + 舊 client、舊 server + 新 client 都能完成任務並收到 3 筆 feedback）。

- [ ] **`_allocate_port` 有 TOCTOU 競爭**
  [server.py:50-53](src/lemegeton/server.py#L50-L53)：先 bind port 0 取號再關閉，真正 bind 之前該 port 可能被別的程序搶走。

- [x] ✅ **`ActionServer` 關閉流程未持鎖遍歷 `active_tasks`**
  worker 完成時會刪除自己的 key，關閉時的迭代可能 `KeyError` 或 `RuntimeError: dictionary changed size during iteration`。
  **已修正**：收尾邏輯整段從 `_listener_loop` 移到 `close()`，並在 `tasks_lock` 內先做一份快照再遍歷。

- [x] ✅ **`ActionServer.close()` 先關 feedback publisher，仍在執行的 worker 會噴錯**
  原本 `_listener_loop` 結束後才 join worker，而 `close()` 又同時去關 `_feedback_pub`，兩者沒有順序保證；join 逾時的 worker 之後送 feedback 會撞到已關閉的 socket。
  **已修正**：關閉順序固定為「停止收單 → 通知並等 worker 收尾（此時 publisher 與 goal socket 都還活著，worker 能正常送出最後的 feedback 與 RESULT）→ 才關閉 publisher 與 socket」。另加三道保護：`close()` 冪等（重複呼叫不再因 socket 已關閉而拋 `ZMQError`）、`close(timeout=...)` 可指定等待上限且 join 全部有界、`_pub_feedback` 與 `_worker_launcher` 在資源已關閉時優雅丟棄並記錄而非拋例外。
  順帶讓 `HeartbeatClient.stop()` 冪等，涵蓋所有服務型別重複 `close()` 的情況。

- [ ] **feedback Publisher 使用 `SNDHWM = 0`（無上限）**
  [server.py:372-378](src/lemegeton/server.py#L372-L378)：沒有訂閱者時佇列會無限成長，長時間執行有記憶體耗盡風險。

- [ ] **`goal_id` 重複未檢查**
  [server.py:458-474](src/lemegeton/server.py#L458-L474)：`active_tasks[goal_id]` 直接覆寫，前一個 worker 的取消旗標與 routing 資訊會遺失（goal_id 由 Client 決定，不能假設一定唯一）。

- [ ] **`Broker` 先註冊到 Gateway 才 `raise NotImplementedError`**
  [server.py:284-290](src/lemegeton/server.py#L284-L290)：心跳線程已啟動且名稱已被占用，卻沒有任何人能關閉它。應在 `super().__init__()` 之前就 raise。

- [ ] **PUB slow-joiner：連線後立即送出的前幾筆訊息必定遺失**
  `client.Publisher` / `server.Publisher` 皆是建立 socket 後馬上可 `send`。ZMQ PUB 在對端完成握手前會直接丟棄訊息。文件需說明，或提供 `wait_for_subscriber()`。

- [ ] **`CONFLATE` 造成靜默丟包**
  [server.py:226](src/lemegeton/server.py#L226)、[client.py:305](src/lemegeton/client.py#L305)：Subscriber 只保留最新一筆。對狀態類資料合理，但對事件/指令類資料會無聲遺失。建議改為可設定，預設值於文件明示。

- [ ] **`client.Publisher.send` / `Requester.send` 無鎖**
  多執行緒呼叫同一個 client 物件即為未定義行為；`Requester` 另有「`send` 與心跳線程同時觸發 `_init_socket`」的競爭（[client.py:142-154](src/lemegeton/client.py#L142-L154)）。

- [x] ✅ ⭐ **導入 blob 大型 payload 傳遞**（0.6.0）
  影像走 protobuf 的 `bytes` 欄位，會產生三次數 MB 的配置與複製（1080p 端到端 4.24ms、4K 22.37ms）。改成「protobuf 只放 header，像素接在同一個 frame 尾端」後，1080p 降到 0.93ms、巢狀情境實測快 3.8 倍，且 schema 完全保留、`CONFLATE` 不用動。
  同時量測了共享記憶體：把 protobuf 的 bytes 包裝拿掉後，單段 zero-copy 與 shm 在 4K 只差 0.03ms、1080p 差 0.19ms（現有 shm 實作並非零複製，本身就有兩次 `np.copyto`）。以框架實際的 blob 實作跨行程複測則是 1080p +1.12ms、4K +2.74ms，詳見下方移除紀錄。
  待辦步驟：
  1. 實作 `msg/common/blob.proto` + `blob.py`（`pack`/`unpack`/`is_blob`）
  2. `Publisher.send(message, blobs=None)`、`Subscriber(unpack_blobs=False)`、`serializer.decode_payload()`
  3. ~~`pyproject.toml` 加入 `numpy` 相依~~ ✅ 已完成
  4. 影像發布端改用 blob，量測實際相機解析度與幀率確認夠用
  5. ~~移除 `shm_util`~~ ✅ 已完成（下游 `sensor_service` 由使用者後續更新，見下方移除紀錄）
  6. README 補上「`CONFLATE` 與 multipart 不相容」的注意事項（未完成）
  7. README 補上 `lemegeton.build()` 與 blob 的使用說明（未完成）

- [ ] ⚠️ **`CONFLATE` 與 multipart 不相容，會靜默破壞資料**（實測確認）
  `CONFLATE=1` 把每個 frame 當成獨立訊息各自 conflate，multipart 的分組被摧毀 —— 送 20 則各 3 frame 的訊息，收端只拿到 **1 則、且只有最後一個 frame**。
  **現有的地雷**：`ActionServer` 的 feedback publisher 送的是 multipart，且以 `{name}_feedback` 正常註冊到 gateway。`ActionFeedbackSubscriber` 沒開 CONFLATE 所以沒事，但使用者若拿一般的 `client.Subscriber` 去訂閱那個名稱（完全合法，因為它就註冊在那裡），由於 `client.Subscriber` 寫死 `CONFLATE=1`，必然收到殘缺的單一 frame。
  **建議**：讓 `Subscriber` 的 CONFLATE 可設定，並在 README 標明。順帶記錄：`SNDHWM`/`RCVHWM` 與 multipart **相容**（以訊息則數計算，實測 `SNDHWM=5` 時不論每則幾個 frame 都剛好卡在 5 則），這點常被誤解。

- [x] ✅ ⚠️ **`shm_util` 已移除**（2026-08-13）
  最初判斷它是死碼，但那次只掃了本 repo。實際呼叫點在 `platform/backpack`：

  | 角色 | 檔案 |
  | --- | --- |
  | 生產端 | `sensor_service/src/solsensor/core/utils/interface_host.py`（`ShmHost` + `get_metadata()` + `release()`） |
  | 消費端 | `sensor_service/src/solsensor/service/sensor_manager.py`（`open_reader()`） |
  | 消費端 | `control_arbitration_service/libs/sensor_api/src/solsensor_api/image_client.py`（`ShmReader`，且已有 `Subscriber` 備援路徑） |

  **決議**：本 repo 先行移除，`sensor_service` 端由使用者後續更新。
  刪除前做了跨行程實測，確認 blob 取代 shm 的成本可接受：

  | 解析度 | shm（寫+讀） | blob（生產者） | 差距 | 佔 30fps 預算 |
  | --- | --- | --- | --- | --- |
  | 720p | 0.13ms | 0.54ms | +0.46ms | 1.6% |
  | 1080p | 0.22ms | 1.26ms | +1.12ms | 3.8% |
  | 4K | 2.49ms | 4.31ms | +2.74ms | 12.9% |

  ⚠️ **4K 高幀率要留意**：4K@60fps 時 blob 會佔到約 26% 的單幀預算。
  遷移的有利條件：`image_client.py` 已有「shm 或 ZMQ topic」雙路徑，可沿用同一個分支結構。

---

## P2 — 專案、建置與工具

- [ ] **使用者 proto 互相 import 會產生錯不到的模組路徑**（實測確認）
  `compile_protos.sh` 用 `-I$PROTO_SRC`（即 `-Imsg`），descriptor 路徑相對於 `msg/`，但輸出卻放進 `lemegeton/msg/...` 這個 package，兩者對不起來。使用者 proto 引用**內建** proto 沒問題（那條路徑本來就是 `lemegeton/msg/...`），但**使用者 proto 引用另一個使用者 proto** 就會壞掉 —— 例如 `import "teleop/teleop.proto"` 產生的是 `from teleop import teleop_pb2`，而安裝後的模組其實在 `lemegeton.msg.teleop.teleop_pb2`。
  目前 repo 裡剛好沒有這種引用，屬潛伏問題。**建議**：讓使用者 proto 的 include 路徑與輸出 package 一致（例如一律以 `lemegeton/msg/` 為根）。

- [ ] **`compile_protos.sh` 編譯失敗仍回傳成功（已實測確認）**
  [compile_protos.sh:21-36](src/lemegeton/compile_protos.sh#L21-L36)：`find | while read` 讓迴圈跑在子 shell，裡面的 `exit 1` 只結束子 shell，腳本仍以 rc=0 結束。實測：子 shell 回 1，但腳本最終 `rc=0`。
  **建議**：改用 `while read; do ... done < <(find ...)` 或收集失敗數後統一 `exit`。

- [ ] **`compile_protos` 把產物寫進 site-packages**
  同上腳本以 `--python_out="$LEMEGETON_PATH/msg"` 直接改寫已安裝套件：升級/重裝會被清空、唯讀環境（容器映像、系統 Python）會失敗、也無法納入版本控管。建議輸出到專案內目錄並以 namespace package 或環境變數掛載。

- [ ] **`MANIFAST.in` 檔名拼錯**（應為 `MANIFEST.in`）→ setuptools 完全不會讀取，sdist 內容可能缺檔。

- [ ] **`pyproject.toml` 設定缺漏**
  ~~`shm_util` 需要 `numpy` 但未列入 `dependencies`~~（✅ 已加入）；`testpaths = ["tests"]` 但實際目錄是 `test/`；`description` 為空；`build-system` 要求 `setuptools_scm` 卻使用靜態 `version`。

- [ ] **全專案以 `print` 輸出，且多處關鍵訊息被註解掉**
  例如 [client.py:63-96](src/lemegeton/client.py#L63-L96)、[gateway.py:330-339](src/lemegeton/gateway.py#L330-L339)：連線失敗、註冊被拒等重要事件在正式環境完全無跡可循。建議改用 `logging` 並分級。

- [ ] **錯誤型別過於籠統**
  大量 `raise Exception(f"...")`，呼叫端無法區分「Gateway 沒回應」「名稱被占用」「bind 失敗」。建議定義 `LemegetonError` 階層。

- [ ] **`test/` 是手動腳本而非自動化測試**
  且 [test_responder.py:25](test/test_responder.py#L25)、[test_action_server.py:36](test/test_action_server.py#L36) 有 `input = input(...)` 覆寫內建函式的錯誤（迴圈第二輪即 `TypeError`）。建議補上以 inproc/ipc 為主的 pytest 整合測試。

- [ ] **沒有 CI、lint、型別檢查**：已設定 black / isort 但沒有任何強制流程。

- [ ] **abstract IPC（`@` 前綴）僅 Linux 支援**
  註冊與心跳皆走 `ipc://@lemegeton_*`，macOS / Windows 無法使用。文件需標明平台限制，或提供 TCP 註冊備援。

- [ ] **沒有 LICENSE 檔案**。

---

## 安全性與部署風險

- [ ] **Gateway 查詢埠無認證、無加密**
  [gateway.py:66-69](src/lemegeton/gateway.py#L66-L69) `tcp://*:60001`：同網段任何人都能枚舉服務名稱與實際 endpoint，等於整個系統的拓樸地圖。

- [ ] **服務註冊表可被任意寫入**
  註冊 / 註銷只靠 `name` + 自報的 `service_id`，沒有任何憑證。惡意行程可搶先占用名稱造成 DoS，或註冊自己的 endpoint 讓 Client 連到假服務（中間人）。

- [x] ✅ **Redis 無密碼且使用 `network_mode: host`**
  `svc:*` 可被同網段任意讀寫，直接繞過 Gateway 竄改路由資訊（把 client 導向任意 endpoint）。
  **已隨 Redis 移除而消失** —— 註冊表不再有無認證的外部副本。

- [ ] **容器使用 `privileged: true` 並掛載 `/dev`**
  [docker-compose.yml](docker-compose.yml)、[deploy/docker-compose.yml](deploy/docker-compose.yml)：容器逃逸風險；Gateway 服務本身並不需要這些權限。

- [ ] **反序列化沒有訊息大小上限**
  所有 socket 皆未設定 `zmq.MAXMSGSIZE`，單一超大訊息即可造成記憶體耗盡。

- [ ] **`deploy/Dockerfile` 缺 `protobuf-compiler`，且 `apt autoremove` 前沒有 `apt update`**
  [deploy/Dockerfile](deploy/Dockerfile)：映像內無法執行 `compile_protos`；compose 又以 `..:/root/workspace` 覆蓋掉 `pip install .` 的來源目錄，實際跑的是掛載進去的原始碼而非安裝版本，行為容易不一致。

- [ ] **硬體假設**
  根目錄 `docker-compose.yml` 寫死 `runtime: nvidia`，無 GPU 的機器無法啟動這個純 Python 開發容器。
  （原本還有「Gateway 沒有 `depends_on: redis` 的啟動順序問題」，已隨 Redis 移除而消失。）

---

## 部署與版本落差（2026-08-12 對現存 `lemegeton-gateway` 實測）

- [x] ✅ ⚠️ **部署映像含有未進版控的修改，重建映像會造成回退**
  `lemegeton-gateway` 映像內 `/usr/local/lib/python3.10/site-packages/lemegeton/` 比
  `src/lemegeton/` 多了兩處 repo 沒有的程式碼：
  1. `Gateway` 的 REGISTER 分支：`last_seen` 超過 TTL 的舊條目視同過期，允許新 `service_id` 接管。
  2. `HeartbeatClient.__init__`：註冊失敗改為重試至 `2 × HEARTBEAT_RATE + 5` 秒才放棄，避免服務快速重啟時直接 crash 進 restart loop。
  **已修正**：兩段皆已從映像回填至 [gateway.py](src/lemegeton/gateway.py)（逐字比對，只有換行格式差異）。**尚未提交進 git**。

- [x] ✅ **執行中的 gateway 已換成含修正的映像**
  修正前以「只存在於 Redis、不在 local_cache」的探針名稱查詢，回應為
  `{"status":"FOUND","data":{"data":{...},"service_id":...,"last_seen":...}}`，
  client 讀到的 `type` / `endpoint` 皆為 `None` —— 代表**只要這座 gateway 重啟過**
  （local_cache 清空、資料只剩 Redis），所有服務就會查不到。重建映像後複測已正常。
  **重建方式備忘**：gateway 用的是 `deploy/docker-compose.yml`（映像 `lemegeton-gateway`）。
  在**專案根目錄**執行 `docker compose build` 只會建到根目錄那份 compose 的 `test` 服務
  （映像 `lemegeton-test`），gateway 不會被換掉：

  ```bash
  cd deploy && docker compose up -d --build
  ```

### 端到端實測結果（對接現存 gateway）

修正前 = 舊映像 `site-packages`；修正後 = 重建後的部署映像本身（未掛 `PYTHONPATH`）。

| 項目 | 修正前 | 修正後 |
| --- | --- | --- |
| Gateway Redis 回填查詢 | `data` 多包一層，`type`/`endpoint` = `None` | ✅ 回傳服務資料本身；回填快取後二次查詢一致；查無服務回 `NOT_FOUND` |
| Responder callback 拋例外後續請求 | `boom→None`，之後 `ok2`/`ok3` 全部 `None`，服務永久失效 | ✅ `boom→""`，`ok2`/`ok3` 正常回覆 |
| Action client 閒置 6 秒（feedback timeout 3 秒） | feedback / result 線程皆死亡，`send_goal` 拋 `Socket operation on non-socket` | ✅ 兩條線程存活，3/3 feedback + `SUCCEEDED` |
| 服務 `kill -9` 後同名重啟（回填碼回歸測試） | — | ✅ 重試 9 次後於 18.0s 接管成功 |

### Client 端連線競態（第二輪，於獨立容器對接同一座 gateway）

| 測試 | 修正前 | 修正後 |
| --- | --- | --- |
| T1 服務尚未上線就建立 client | `TypeError: expected bytes, NoneType found`（`connect(None)`） | ✅ 可建構；未連線時 `send_goal` 拋 `ConnectionError`；服務上線後 Subscriber / ActionClient 自動接上 |
| T2 8 執行緒併發 32 個 goal ＋ 8 次併發 cancel | DEALER 無鎖，行為未定義 | ✅ 32/32 完成（24 SUCCEEDED / 8 CANCELED），0 例外，1.7s |
| T3 `docker restart lemegeton-gateway` | 心跳線程死亡，服務換 port 後永遠追不上 | ✅ 心跳/接收線程全存活；停機期間既有 PUB/SUB 不受影響；服務換 port 後 Requester `43961→44179`、Subscriber `42381→48067`，13.1s 內全部恢復 |
| T4 gateway 不可達（query_port 指向空埠） | 2 秒後 `Heartbeat error: Operation cannot be accomplished in current state`，線程終止 | ✅ 持續重試，6 秒觀察期間線程恆存活 |
| T5 ActionServer `mode="ipc"` / `"both"` | 兩種模式都失敗：client 解析到 ipc endpoint，`send_goal` 不報錯但結果永不回來（`TimeoutError`） | ✅ 兩種模式皆 `SUCCEEDED`，feedback 也經 ipc 正常送達 |
| T6 同名衝突（service_id 不同的實例對真實 gateway 心跳） | gateway 確實回 `MISMATCH`，但冒名者 6 秒後心跳線程仍存活，持續覆寫他人註冊 | ✅ 立即偵測、印出警告、`name_conflict=True`、線程自行結束 |
| T7 存活探測單元測試（真實 zmq socket，9 種情境 + 3 種註冊分支） | — | ✅ 12/12：端點被佔用→活著、端點已關→已死、未過 grace 不探測、活著時不可接管、超過 TTL 可接管 |
| T8 `kill -9` 後同名重啟（部署版） | 重試 9 次、**18.0s** 才接管，離 25s 上限只剩 7s | ✅ **0.0s** 立即接管；服務仍活著時同名註冊照樣被拒 |
| T9 移除 Redis 後的全套驗證（`--network none` 隔離容器） | — | ✅ gateway 無外部相依即可啟動；殺掉再重啟後服務 **6.5s** 靠心跳自行重新註冊；停機期間既有連線不受影響 |
| T10 隔離環境崩潰接管（`mode="both"`，tcp + ipc 雙探測） | — | ✅ 活著時拒絕、崩潰後 0.0s 接管、新服務順利綁到自己的 port（探測未留殘影） |
| T11 feedback 完整性（callback 每筆 40ms，RESULT 緊接最後一筆之後） | 每個任務 3 筆只收到 **1 筆**（6/18） | ✅ **18/18**，且沒有任何 feedback 落在 `result_callback` 之後 |
| T11 關閉與資源回收（worker 活得比 close timeout 久） | `close()` 重複呼叫 → `ZMQError: Socket operation on non-socket` | ✅ 冪等、worker 全數收尾、關閉後的 feedback 優雅丟棄 |
| T12 跨版本相容（feedback 加 seq、RESULT 加總數） | — | ✅ 新 server + 舊 client、舊 server + 新 client 都 `SUCCEEDED` 且收到 3 筆 feedback |

> 上表「修正後」欄位自 T5 起皆為重建映像後的**部署版本**實測（未掛 `PYTHONPATH`）。
> T1、T2 已於同一版本複測通過，確認 gateway 註冊路徑的改動沒有造成回歸。

> 同名重啟要等 **18 秒**才成功（重試上限 `2 × HEARTBEAT_RATE + 5 = 25` 秒），對需要快速重啟的服務
> 偏長且離上限不遠。可考慮縮短 TTL 或提高心跳頻率，見上方 P1「服務崩潰後 20 秒內無法以同名重啟」。

---

## 建議修復順序

1. ~~**先修 P0 中會讓服務「靜默失效」的三項**：Gateway Redis 回填格式（服務永遠找不到）、`ActionFeedbackSubscriber` 閒置逾時、`Responder` 例外不回覆。~~ ✅ 已完成
2. 接著補齊 **Client 端連線競態**（endpoint 為 None、DEALER 併發、心跳線程死亡），這幾項決定框架能否長時間穩定運行。
3. 再處理 **關閉與資源回收**（`ActionClient.close`、`ActionServer.close`、`Broker`）。
4. 最後是建置與部署（`compile_protos.sh`、`MANIFEST.in`、容器權限）。
