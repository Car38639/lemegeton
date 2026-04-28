from lemegeton.msg.common.std_msgs_pb2 import String

import lemegeton


def message_callback(msg):
    print("Received message:", msg.value)


context = lemegeton.Context()

#####################################################################

# sub = lemegeton.server.Subscriber(
#     context=context,
#     name="test_pub",
#     message_class=String,
#     callback=message_callback,
#     mode="ipc",  # 使用 TCP 模式
# )


sub = lemegeton.server.Subscriber(
    context=context,
    name="test_pub",
    message_class=String,
    callback=message_callback,
    ip_address="localhost",  # 指定服務所在的 IP 地址
    timeout=1.0,  # 設定查詢服務的超時時間 seconds
)

#####################################################################

try:
    print("Subscriber is running. Press Ctrl+C to stop.")
    while True:
        pass
except KeyboardInterrupt:
    print("\n正在關閉 Subscriber...")
finally:
    sub.close()
    context.term()
    print("Subscriber 已關閉")
