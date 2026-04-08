import zmq
from lemegeton.msg.common.std_msgs_pb2 import Int, String

from lemegeton.zmq_protocol import (
    ZmqProtobufRouter,
)

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()
    router = ZmqProtobufRouter(
        context=context,
        port=60001,
    )

    def test_task_callback(message):
        print(f"Receive message: {message.value}")
        response = String(value=f"Finish handling message: {message.value}")
        return response

    router.register_worker(
        name="test",
        description="Handles work for the task name 'test'.",
        message_class=Int,
        callback=test_task_callback,
        response_class=String,
    )

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        router.close()
