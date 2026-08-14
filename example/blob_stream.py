"""大型數值資料（影像）的傳送與讀取範例。

延續 `lemegeton.build()` 的寫法 —— **ndarray 直接指派給 Blob 欄位就好**，
框架會依大小自動決定要內嵌進訊息還是搬到訊息體外::

    frame = lemegeton.build(Image)
    frame.data = rgb                 # ndarray，shape/dtype 自動填
    publisher.send(frame)

收端加上 ``unpack_blobs=True``，callback 就會拿到 ``(message, arrays)``，
不論發端選了哪種模式都一樣::

    def on_frame(msg, arrays):
        rgb = arrays["data"]         # ndarray

執行方式::

    python3 deploy/main.py &          # 先啟動 gateway
    python3 example/blob_stream.py
"""

import time

import numpy as np

import lemegeton
from lemegeton import blob
from lemegeton.msg.sensor.image_pb2 import Image


def section(title):
    print(f"\n{'─' * 4} {title} {'─' * max(4, 58 - len(title))}")


# --------------------------------------------------------------------------
# 送出端
# --------------------------------------------------------------------------
def publish_frame(publisher, capture) -> bool:
    """從 OpenCV 讀一幀並送出。"""
    ok, image = capture.read()          # (H, W, 3) uint8，OpenCV 是 BGR
    if not ok:
        return False

    frame = lemegeton.build(Image)
    frame.camera_id = "head_rgb"
    frame.encoding = "bgr8"
    frame.shape = list(image.shape)     # 原始尺寸；壓縮送出時這是唯一的來源
    frame.data = image                  # ← ndarray 直接指派，shape/dtype 自動填
    publisher.send(frame)
    return True


# --------------------------------------------------------------------------
# 讀取端
# --------------------------------------------------------------------------
def on_frame(msg: Image, arrays: dict):
    """unpack_blobs=True 時 callback 的簽章是 (message, arrays)。

    ⚠️ 資料搬出訊息體時，arrays 裡是接收緩衝的 view（零複製），
       只在這個 callback 內有效。要保留請自行 .copy()。
    """
    image = arrays.get("data")
    if image is None:
        print("   （這一幀沒有影像）")
        return

    print(f"   camera={msg.camera_id!r} encoding={msg.encoding!r} "
          f"shape={image.shape} dtype={image.dtype} "
          f"左上角像素={tuple(int(v) for v in image[0, 0])}")


# --------------------------------------------------------------------------
# 內嵌 vs 搬出訊息體
# --------------------------------------------------------------------------
def two_modes():
    section("同一份 schema，兩種傳輸模式")

    print(f"   門檻 = {blob.THRESHOLD // 1024} KB（實測的交叉點）\n")

    for label, image in (("縮圖 100x100", np.zeros((100, 100, 3), np.uint8)),
                         ("1080p", np.zeros((1080, 1920, 3), np.uint8))):
        frame = lemegeton.build(Image, camera_id="cam", encoding="bgr8")
        frame.data = image
        message, arrays = frame.build_payload()
        payload = blob.encode(message, arrays)

        mode = "搬出訊息體" if blob.is_blob_payload(payload) else "內嵌"
        # 兩種模式收端的寫法完全一樣
        got_msg, got_arrays = blob.decode(bytes(payload), Image)
        ok = np.array_equal(got_arrays["data"], image)
        inline_bytes = len(got_msg.data.data)
        print(f"   {label:<12}{image.nbytes/1024:>8.0f} KB → {mode:<10}"
              f"封包 {len(payload)/1024:>8.0f} KB   "
              f"訊息內的 data {inline_bytes/1024:>6.0f} KB   還原正確={ok}")

    print("\n   → 收端一律 arrays[\"data\"]，不必分辨發端用了哪種模式")


def why_it_matters():
    section("為什麼不要把大陣列塞進 protobuf 的 bytes 欄位")
    print("""   實測（1080p RGB，6.2MB，端到端往返）：

       protobuf 內含 bytes    22.4 ms
       搬出訊息體              3.3 ms      6.8 倍

   原因是 tobytes() → 指派欄位 → SerializeToString() 會產生三次數 MB 的
   配置與複製，收端再一次。搬出訊息體之後這些複製都不存在。

   但這個作法有約 10~15µs 的固定開銷，小 payload 反而更慢 ——
   所以框架依 50KB 門檻自動選擇，使用者不必判斷。""")


# --------------------------------------------------------------------------
# 端到端
# --------------------------------------------------------------------------
class _SyntheticCapture:
    """替身相機（介面與 cv2.VideoCapture 相同）"""

    def __init__(self, shape=(1080, 1920, 3)):
        self._frame = np.zeros(shape, dtype=np.uint8)
        self._i = 0

    def read(self):
        self._i += 1
        self._frame[0, 0] = (self._i % 256, 7, 42)
        return True, self._frame

    def release(self):
        pass


def end_to_end():
    section("端到端")

    context = lemegeton.Context()
    image_pub = lemegeton.server.Publisher(
        context=context, name="camera_demo", message_class=Image, mode="both")
    image_sub = lemegeton.client.Subscriber(
        context=context, name="camera_demo", message_class=Image,
        callback=on_frame, ip_address="localhost",
        unpack_blobs=True,               # ← callback 才會收到 (message, arrays)
        connect_timeout=5.0)

    if not image_sub.is_connect():
        print("   （找不到 gateway，跳過；請先執行 python3 deploy/main.py）")
        image_sub.close(); image_pub.close(); context.term()
        return

    capture = _SyntheticCapture()
    for _ in range(3):
        publish_frame(image_pub, capture)
        time.sleep(0.2)
    time.sleep(0.3)
    image_sub.close(); image_pub.close(); context.term()


if __name__ == "__main__":
    two_modes()
    end_to_end()
    why_it_matters()
