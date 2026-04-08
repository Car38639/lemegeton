import zmq
from solproto.msg.common.std_msgs_pb2 import String
from solproto.zmq_protocol import ZmqProtobufSubscriber

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()

    def subscribe_callback(message):
        print(message.value)

    subsrciber = ZmqProtobufSubscriber(
        message_class=String,
        callback=subscribe_callback,
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
        subsrciber.close()
