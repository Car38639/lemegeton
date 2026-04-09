import lemegeton
from lemegeton.msg.common.std_msgs_pb2 import Bool, String

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.

    def respond_callback(message):
        print(f"Receive message: {message.value}")
        response = String(value=f"Finish handling message: {message.value}")
        return response

    gateway = lemegeton.Gateway()
    gateway.register_responder(
        name="test_responder",
        message_class=Bool,
        response_class=String,
        callback=respond_callback,
    )

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        gateway.remove("test_responder")
        gateway.close()
