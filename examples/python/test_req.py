import time

import lemegeton
from lemegeton.msg.common.std_msgs_pb2 import Bool, String

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    requester = lemegeton.create_requester(
        name="test_responder",
        ip_address="localhost",
    )

    key = False
    for idx in range(5):
        message = Bool(value=key)
        key = not key
        response = requester.request(message)
        print(response.value)
        time.sleep(1)

    # Test for wrong request class
    message = String(value="wrong request class")
    response = requester.request(message)
    print(response)

    requester.close()
