import zmq
from solproto.msg.common.std_msgs_pb2 import Bool, String
from solproto.zmq_protocol import (
    ZmqProtobufResponder,
)

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()

    def respond_callback(message):
        print(f"Receive message: {message.value}")
        response = String(value=f"Finish handling message: {message.value}")
        return response

    responder = ZmqProtobufResponder(
        request_class=Bool,
        response_class=String,
        callback=respond_callback,
        context=context,
        port=60001,
    )

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        responder.close()
