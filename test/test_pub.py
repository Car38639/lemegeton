import time

from lemegeton.msg.common.std_msgs_pb2 import String

import lemegeton

context = lemegeton.Context()


#####################################################################


pub = lemegeton.server.Publisher(
    context=context, name="test_pub", message_class=String, mode="tcp"
)


# pub = lemegeton.client.Publisher(
#     context=context, name="test_pub", message_class=String, ip_address="localhost"
# )

#####################################################################

try:
    while True:
        msg = String()
        msg.value = "Hello, Lemegeton!"
        pub.send(msg)
        print("Published message:", msg.value)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n正在關閉 Publisher...")
finally:
    pub.close()
    context.term()
    print("Publisher 已關閉")
