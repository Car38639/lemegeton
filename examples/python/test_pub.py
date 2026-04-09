import time

import lemegeton
from lemegeton.msg.common.std_msgs_pb2 import String

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.

    gateway = lemegeton.Gateway()

    publisher = gateway.register_publisher(name="test_pub", message_class=String)

    for idx in range(5):
        message = String(value=f"Hello world ! Test idx = {idx}")
        gateway.send("test_pub", message)
        print(f"Publish message: {message.value}")
        time.sleep(1)

    gateway.close()
