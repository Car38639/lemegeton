import time

import zmq
from lemegeton.msg.common.std_msgs_pb2 import Bool, String

from lemegeton.zmq_protocol import ZmqProtobufPusher

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()
    pusher = ZmqProtobufPusher(
        message_class=String,
        context=context,
        port=60001,
    )

    for idx in range(5):
        message = String(value=f"Hello world ! Test idx = {idx}")
        pusher.push(message)
        print(f"Push message: {message.value}")
        time.sleep(1)

    # Test for Wrong message type
    message = Bool(value=True)
    pusher.push(message)

    pusher.close()
    time.sleep(0.5)

    # Test for Mismatch message type with puller
    pusher = ZmqProtobufPusher(
        message_class=Bool,
        context=context,
        port=60001,
    )
    for idx in range(5):
        message = Bool(value=True)
        pusher.push(message)
        print(f"Push message: {message.value}")
        time.sleep(1)
    pusher.close()
