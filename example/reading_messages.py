"""讀取端範例：怎麼安全地讀一個收到的訊息。

寫入端有 `lemegeton.build()` 的語法糖（見 example/staging_usage.py），
但**讀取端不需要任何包裝** —— 收到的就是原生 protobuf 訊息，直接讀最快。

真正要小心的是 protobuf 的語意：`oneof` 該用 `WhichOneof()` 判斷、
proto3 的純量沒有「有沒有設定」的概念、repeated 欄位批次讀取比逐個取快很多。

執行方式::

    python3 deploy/main.py &              # 先啟動 gateway
    python3 example/reading_messages.py
"""

import time

import lemegeton
from lemegeton.msg.humanoid.robot_control_pb2 import (
    Arm,
    DataMode,
    HandGrasp,
    Locomotion,
    ModularControl,
    RobotControl,
)


def section(title):
    print(f"\n{'─' * 4} {title} {'─' * max(4, 58 - len(title))}")


# --------------------------------------------------------------------------
# 讀取端的 callback
# --------------------------------------------------------------------------
_frame_count = 0


def on_control(msg: RobotControl):
    """Subscriber 的 callback。msg 是原生 protobuf 訊息，直接讀即可。"""
    global _frame_count
    _frame_count += 1
    if _frame_count > 1:                    # 後續的幀只印一行，避免洗版
        print(f"   （第 {_frame_count} 幀，內容相同）")
        return

    # ── 1. 純量與巢狀純量：直接取 ──
    print(f"   robot_type   = {msg.robot_type!r}")
    print(f"   control_mode = {RobotControl.ControlMode.Name(msg.control_mode)}")

    # ── 2. timestamp：轉成 datetime ──
    if msg.HasField("timestamp"):
        print(f"   timestamp    = {msg.timestamp.ToDatetime():%H:%M:%S.%f}")

    # ── 3. oneof：一定要用 WhichOneof，不要用 HasField 猜 ──
    limb = msg.WhichOneof("limb")          # "mc" / "wbc" / None
    print(f"   limb 用的是   = {limb!r}")

    if limb == "mc":
        read_modular(msg.mc)
    elif limb == "wbc":
        print(f"     wbc.dof = {msg.wbc.dof}")
    else:
        print("     （這一幀沒有帶 limb 指令）")

    # 巢狀 oneof：左右手各自獨立
    for side in ("left", "right"):
        which = msg.WhichOneof(f"{side}_hand_control")
        if which is None:
            continue
        value = getattr(msg, which)
        if isinstance(value, HandGrasp):
            print(f"   {side}: 抓握指令 enable={value.enable} value={value.value:.2f}")
        else:
            print(f"   {side}: 逐關節指令 type={value.type!r} dof={value.dof}")


def read_modular(mc: ModularControl):
    # ── 4. HasField 只能用在「子訊息」與 oneof 欄位上 ──
    if mc.HasField("arm"):
        read_arm(mc.arm)
    if mc.HasField("locomotion"):
        read_locomotion(mc.locomotion)


def read_arm(arm: Arm):
    # 列舉：用 Name() 轉成可讀字串，不要直接印數字
    print(f"     arm: enable={arm.enable} dof={arm.dof} "
          f"data_mode={DataMode.Name(arm.data_mode)}")

    which = arm.WhichOneof("arm")
    if which == "jointstate":
        # ── 5. repeated 純量：整包轉 list，不要逐個 index ──
        positions = list(arm.jointstate.positions)     # 一次取完
        print(f"     jointstate.positions = {[round(p, 3) for p in positions]}")
        # 若有 numpy：np.frombuffer 不適用，但 np.asarray(positions) 只複製一次
    elif which == "pose":
        p = arm.pose.position
        print(f"     pose.position = ({p.x:.2f}, {p.y:.2f}, {p.z:.2f})")


def read_locomotion(loco: Locomotion):
    v = loco.twist.linear
    print(f"     locomotion: enable={loco.enable} "
          f"linear=({v.x:.2f}, {v.y:.2f}) waist_height={loco.waist_height:.2f}")


# --------------------------------------------------------------------------
# proto3 的兩個容易踩的語意
# --------------------------------------------------------------------------
def presence_semantics():
    section("proto3 的 presence：純量分不出「沒設定」與「設成 0」")

    msg = RobotControl()
    print(f"   全新訊息 robot_type = {msg.robot_type!r}（不是 None，是空字串）")
    print(f"   全新訊息 control_mode = {msg.control_mode}（不是 None，是列舉的 0）")

    msg.robot_type = ""
    print(f"   明確設成空字串之後仍然是 {msg.robot_type!r} —— 兩者無法區分")

    print("\n   子訊息與 oneof 才有 presence：")
    print(f"     msg.HasField('mc')        = {msg.HasField('mc')}")
    print(f"     msg.WhichOneof('limb')    = {msg.WhichOneof('limb')!r}")
    msg.mc.arm.dof = 7                       # 一碰到就算「有設定」
    print(f"     碰過 msg.mc.arm.dof 之後：HasField('mc') = {msg.HasField('mc')}")

    print("\n   → 需要區分「沒帶這個欄位」時，把它包成子訊息或放進 oneof，")
    print("     不要靠純量的預設值判斷。")


def reading_vs_writing():
    section("收到的訊息可以改嗎")

    msg = RobotControl(robot_type="humanoid")
    msg.mc.arm.jointstate.positions.extend([0.1] * 7)

    # 可以：純量、巢狀純量
    msg.robot_type = "humanoid-v2"
    msg.mc.arm.dof = 7
    print(f"   ✅ 純量可直接改：robot_type = {msg.robot_type!r}, arm.dof = {msg.mc.arm.dof}")

    # 可以：repeated 的原地操作
    del msg.mc.arm.jointstate.positions[:]
    msg.mc.arm.jointstate.positions.extend([0.2] * 7)
    print(f"   ✅ repeated 可原地改：positions[0] = {msg.mc.arm.jointstate.positions[0]}")

    # 不行：整包指派子訊息 / repeated
    try:
        msg.mc.locomotion = Locomotion(enable=True)
    except AttributeError as e:
        print(f"   ❌ 整包指派子訊息：{str(e)[:48]}")
    msg.mc.locomotion.CopyFrom(Locomotion(enable=True))   # 要用 CopyFrom
    print(f"   → 改用 CopyFrom：locomotion.enable = {msg.mc.locomotion.enable}")

    print("\n   註：lemegeton.build() 的鷹架只用於「組裝新訊息」，")
    print("       收到的訊息要改就依上面的規則，或整包重建一個。")


# --------------------------------------------------------------------------
# 端到端
# --------------------------------------------------------------------------
def end_to_end():
    section("端到端：Subscriber 收到之後怎麼讀")

    context = lemegeton.Context()
    publisher = lemegeton.server.Publisher(
        context=context, name="control_demo", message_class=RobotControl, mode="both")
    subscriber = lemegeton.client.Subscriber(
        context=context, name="control_demo", message_class=RobotControl,
        callback=on_control, ip_address="localhost", connect_timeout=5.0)

    if not subscriber.is_connect():
        print("   （找不到 gateway，跳過這一節；請先執行 python3 deploy/main.py）")
        subscriber.close(); publisher.close(); context.term()
        return

    # 用寫入端的語法糖組一個指令
    cmd = lemegeton.build(RobotControl)
    cmd.robot_type = "humanoid"
    cmd.control_mode = RobotControl.MC
    cmd.mc.arm.enable = True
    cmd.mc.arm.dof = 7
    cmd.mc.arm.jointstate.positions = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]
    cmd.mc.locomotion.enable = True
    cmd.mc.locomotion.twist.linear.x = 0.5
    cmd.mc.locomotion.waist_height = 0.9
    cmd.left_grasp = HandGrasp(enable=True, value=0.75)
    proto = cmd.to_proto()
    proto.timestamp.GetCurrentTime()

    for _ in range(3):
        publisher.send(proto)
        time.sleep(0.2)
    time.sleep(0.3)

    subscriber.close(); publisher.close(); context.term()


if __name__ == "__main__":
    end_to_end()
    presence_semantics()
    reading_vs_writing()
