import time

import zmq
from solproto.msg.common.std_msgs_pb2 import Bool, String
from solproto.zmq_protocol import ZmqProtobufRequester

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()
    requester = ZmqProtobufRequester(
        request_class=Bool,
        response_class=String,
        context=context,
        ip_address="localhost",
        port=60001,
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
