import time

import zmq
from solproto.msg.common.std_msgs_pb2 import Int, String
from solproto.zmq_protocol import ZmqProtobufDealer, request_router_info

if __name__ == "__main__":
    # If you run multiple ZmqProtobuf protocols in the same Process,
    # you should use the same zmq.Context()
    # otherwise it might cause trouble.
    context = zmq.Context()

    router_info = request_router_info(
        ip_address="localhost",
        port=60001,
    )

    print(f"Router Info: \n{router_info}\n")

    dealer = ZmqProtobufDealer(
        message_class=Int,
        response_class=String,
        context=context,
        ip_address="localhost",
        port=60001,
        task_name="test",
    )

    for idx in range(5):
        message = Int(value=idx)
        response = dealer.send(message)
        print(response.value)
        time.sleep(1)

    # Test for wrong message type
    message = String(value="wrong message class")
    response = dealer.send(message)
    print(response)

    dealer.close()
    time.sleep(0.5)

    # Test for mismatch message type with puller
    dealer = ZmqProtobufDealer(
        message_class=String,
        response_class=String,
        context=context,
        ip_address="localhost",
        port=60001,
        task_name="test",
    )
    for idx in range(5):
        message = String(value="mismatch message class")
        response = dealer.send(message)
        print(response)
        time.sleep(1)
    dealer.close()
