import lemegeton

context = lemegeton.Context()

from lemegeton.msg.common.std_msgs_pb2 import String


def message_callback(msg):
    print("Received message:", msg.value)
    res = String(value=f"Responder reseive:{msg.value}, hello back!")
    return res


responder = lemegeton.server.Responder(
    context=context,
    name="test_responder",
    message_class=String,
    response_class=String,
    callback=message_callback,
    mode="both",
)

try:
    while True:
        input = input("Press q + enter to exit......")
        break
except KeyboardInterrupt:
    pass
finally:
    responder.close()
