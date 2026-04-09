import time

import lemegeton
from lemegeton.msg.common.std_msgs_pb2 import Bool, String

if __name__ == "__main__":
    gateway = lemegeton.Gateway()
    gateway.register_pusher(
        name="test_pusher",
        message_class=String,
    )

    for idx in range(5):
        message = String(value=f"Hello world ! Test idx = {idx}")
        gateway.send("test_pusher", message)
        print(f"Push message: {message.value}")
        time.sleep(1)

    # Test for Wrong message type
    message = Bool(value=True)
    gateway.send("test_pusher", message)

    gateway.remove("test_pusher")
    time.sleep(0.5)

    # Test for Mismatch message type with puller
    gateway.register_pusher(
        name="test_pusher",
        message_class=Bool,
    )
    for idx in range(5):
        message = Bool(value=True)
        gateway.send("test_pusher", message)
        print(f"Push message: {message.value}")
        time.sleep(1)
    gateway.remove("test_pusher")

    gateway.close()
