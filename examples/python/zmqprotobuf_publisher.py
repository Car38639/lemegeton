import time

import zmq
from solproto.msg.common.std_msgs_pb2 import Bool, String
from solproto.zmq_protocol import ZmqProtobufPublisher

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()
    publisher = ZmqProtobufPublisher(
        message_class=String,
        context=context,
        port=60001,
    )

    for idx in range(5):
        message = String(value=f"Hello world ! Test idx = {idx}")
        publisher.publish(message)
        print(f"Publish message: {message.value}")
        time.sleep(1)

    # Test for wrong message type
    message = Bool(value=True)
    publisher.publish(message)

    publisher.close()
