import zmq
from lemegeton.msg.common.std_msgs_pb2 import String

from lemegeton.zmq_protocol import ZmqProtobufPuller

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()

    def pull_callback(message):
        print(message.value)

    puller = ZmqProtobufPuller(
        message_class=String,
        callback=pull_callback,
        context=context,
        ip_address="localhost",
        port=60001,
    )

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        puller.close()
