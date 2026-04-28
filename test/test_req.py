import time

from lemegeton.msg.common.std_msgs_pb2 import String

import lemegeton

context = lemegeton.Context()


req = lemegeton.client.Requester(
    context=context,
    name="test_responder",
    message_class=String,
    response_class=String,
    ip_address="localhost",  # 指定服務所在的 IP 地址
    timeout=3.0,
)

try:
    while True:
        msg = String()
        msg.value = "Hello, Lemegeton!"
        res = req.send(msg)
        if res:
            print("Responded message:", res.value)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n正在關閉 Requester...")
finally:
    req.close()
    context.term()
    print("Requester 已關閉")
