import zmq

context = zmq.Context()

from lemegeton.msg.common.std_msgs_pb2 import String
from lemegeton.server import Responder


def message_callback(msg):
    print("Received message:", msg.value)
    res = String(value=f"Responder reseive:{msg.value}, hello back!")
    return res


responder = Responder(
    context=context,
    name="test_responder",
    message_class=String,
    response_class=String,
    callback=message_callback,
    mode="both",
    worker_num=4,
)

try:
    while True:
        input = input("Press q + enter to exit......")
        break
except KeyboardInterrupt:
    pass
finally:
    responder.close()
