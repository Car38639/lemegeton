"""事後指派語法糖（`lemegeton.build`）的使用範例。

protobuf 的 repeated 與子訊息欄位不允許事後指派，只能用建構子或
`extend()` / `CopyFrom()`。`lemegeton.build()` 提供一層「組裝用的鷹架」，
讓事後指派可行，送出時再一次轉成真正的 protobuf 訊息。

執行方式（第 1~4 節不需要 gateway，第 5 節需要）::

    python3 deploy/main.py &            # 先啟動 gateway
    python3 example/staging_usage.py
"""

import time

import lemegeton
from lemegeton.msg.common.geometry_pb2 import Pose, Quaternion, Vector3
from lemegeton.msg.common.std_msgs_pb2 import String
from lemegeton.msg.sensor.image_pb2 import Image
from lemegeton.msg.teleop.teleop_pb2 import TeleopData, TeleopHeader


def section(title):
    print(f"\n{'─' * 4} {title} {'─' * (60 - len(title))}")


# --------------------------------------------------------------------------
# 1. protobuf 原本擋掉什麼
# --------------------------------------------------------------------------
def the_problem():
    section("1. protobuf 原本擋掉的寫法")

    image, teleop = Image(), TeleopData()
    for label, action in (
        ("msg.shape = [1080, 1920, 3]",
         lambda: setattr(image, "shape", [1080, 1920, 3])),          # repeated 純量
        ("msg.pelvis = Pose()",
         lambda: setattr(teleop, "pelvis", Pose())),                 # 子訊息
        ("msg.left_dexterous = [Pose()]",
         lambda: setattr(teleop, "left_dexterous", [Pose()])),       # repeated 子訊息
    ):
        try:
            action()
            print(f"   {label:<34} 竟然成功了？")
        except AttributeError as e:
            print(f"   {label:<34} ❌ {str(e)[:46]}")

    print("   msg.dtype = 'uint8'                ✅ 純量欄位本來就可以")


# --------------------------------------------------------------------------
# 2. 基本用法：三種原本被擋的指派
# --------------------------------------------------------------------------
def basic_assignment():
    section("2. 用 lemegeton.build() 之後")

    msg = lemegeton.build(Image)
    msg.shape = [1080, 1920, 3]      # repeated 純量（原本要 extend）
    msg.dtype = "uint8"              # 純量
    proto = msg.to_proto()           # 轉成真正的 protobuf 訊息
    print(f"   Image  shape={list(proto.shape)} dtype={proto.dtype!r}")

    teleop = lemegeton.build(TeleopData)
    teleop.pelvis = Pose(position=Vector3(x=1, y=2, z=3))       # 子訊息（原本要 CopyFrom）
    teleop.left_dexterous = [Pose(orientation=Quaternion(w=1))  # repeated 子訊息
                             for _ in range(25)]
    proto = teleop.to_proto()
    print(f"   TeleopData  pelvis.x={proto.pelvis.position.x} "
          f"left_dexterous={len(proto.left_dexterous)} 個")


# --------------------------------------------------------------------------
# 3. 巢狀：不必先建子物件
# --------------------------------------------------------------------------
def nested_assignment():
    section("3. 巢狀欄位自動建立")

    msg = lemegeton.build(TeleopData)
    msg.header.device_type = "hand_controller"   # header 自動建立
    msg.header.frame_id = 7
    msg.pelvis.position.x = 1.5                  # 兩層都自動建立
    msg.pelvis.position.y = 2.5
    msg.pelvis.orientation.w = 1.0

    proto = msg.to_proto()
    print(f"   header.device_type = {proto.header.device_type!r}")
    print(f"   pelvis.position    = ({proto.pelvis.position.x}, {proto.pelvis.position.y})")
    print(f"   沒碰過的 waist 欄位不會被送出：HasField('waist') = {proto.HasField('waist')}")

    # 與原生建構子的結果完全相同
    native = TeleopData(
        header=TeleopHeader(device_type="hand_controller", frame_id=7),
        pelvis=Pose(position=Vector3(x=1.5, y=2.5), orientation=Quaternion(w=1.0)))
    print(f"   與原生建構子的 bytes 相同：{native.SerializeToString() == proto.SerializeToString()}")


# --------------------------------------------------------------------------
# 4. 與原生寫法混用
# --------------------------------------------------------------------------
def mixing():
    section("4. 兩種寫法可以混用")

    # 熱路徑用原生建構子（最快），週邊欄位用鷹架補
    poses = [Pose(orientation=Quaternion(w=1)) for _ in range(25)]

    msg = lemegeton.build(TeleopData)
    msg.left_dexterous = poses                       # 直接塞原生訊息
    msg.header = TeleopHeader(device_type="hand")    # 也可以整包指派
    msg.pelvis.position.z = 0.9                      # 再用鷹架補一個欄位

    proto = msg.to_proto()
    print(f"   left_dexterous={len(proto.left_dexterous)} 個  "
          f"device_type={proto.header.device_type!r}  pelvis.z={proto.pelvis.position.z}")

    # 打錯欄位名會立刻被擋下，不會等到送出才發現
    try:
        msg.widht = 1920
    except AttributeError as e:
        print(f"   打錯欄位名：{str(e)[:56]}")


# --------------------------------------------------------------------------
# 5. 直接送出：send() 會自動轉換
# --------------------------------------------------------------------------
def send_directly():
    section("5. Publisher 直接接受鷹架物件")

    context = lemegeton.Context()
    received = []

    publisher = lemegeton.server.Publisher(
        context=context, name="staging_demo", message_class=String, mode="both")
    subscriber = lemegeton.client.Subscriber(
        context=context, name="staging_demo", message_class=String,
        callback=lambda m: received.append(m.value),
        ip_address="localhost", connect_timeout=5.0)

    if not subscriber.is_connect():
        print("   （找不到 gateway，跳過這一節；請先執行 python3 deploy/main.py）")
        subscriber.close(); publisher.close(); context.term()
        return

    msg = lemegeton.build(String)
    msg.value = "hello from staging"

    for _ in range(5):
        publisher.send(msg)                       # 不必自己呼叫 to_proto()
        time.sleep(0.1)
    staged = list(received)

    publisher.send(String(value="native message"))  # 原生訊息照常
    time.sleep(0.3)

    print(f"   鷹架物件送出後收到：{staged[-1]!r}")
    print(f"   原生訊息送出後收到：{received[-1]!r}")
    # 註：PUB/SUB 的 slow joiner 特性會讓最初幾筆遺失，與鷹架無關
    subscriber.close(); publisher.close(); context.term()


# --------------------------------------------------------------------------
# 6. 什麼時候不要用
# --------------------------------------------------------------------------
def when_not_to_use():
    section("6. 成本與適用場景")
    print("""   鷹架相對原生建構子的成本（TeleopData，1000Hz 下的單核佔用）：

       原生建構子          11.5 µs   1.00x   1.15%
       鷹架：外層指派      17.0 µs   1.46x   1.70%
       鷹架：巢狀自動建立  24.9 µs   2.13x   2.49%

   → 1000Hz 的熱路徑（teleop、控制迴圈）建議直接用原生建構子
   → 其餘場景（設定、狀態回報、影像 metadata）用鷹架，可讀性划算得多
   → 影像這種欄位少的訊息，鷹架只花 2.0 µs，30fps 下佔 0.006%

   另一個限制：鷹架只用於「組裝」。收到的是原生 protobuf 訊息，
   要修改它仍受 protobuf 原本的限制。""")


if __name__ == "__main__":
    the_problem()
    basic_assignment()
    nested_assignment()
    mixing()
    send_directly()
    when_not_to_use()
