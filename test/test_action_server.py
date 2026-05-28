import lemegeton

context = lemegeton.Context()

import time

from lemegeton.msg.common.std_msgs_pb2 import String


def execute_callback(goal_handle) -> tuple[String, bool]:
    for i in range(20):
        time.sleep(1)
        if goal_handle.is_canceled():
            res = String(value="Action Server Canceled!")
            return res, False

        feedback = String(value=f"Progress {goal_handle.goal.value}: {i + 1}/20")
        goal_handle.send_feedback(feedback)

    res = String(value="Action Server Done!")
    return res, True


action_server = lemegeton.server.ActionServer(
    context=context,
    name="test_action_server",
    goal_class=String,
    result_class=String,
    feedback_class=String,
    callback=execute_callback,
    mode="tcp",
)

try:
    while True:
        input = input("Press q + enter to exit......")
        break
except KeyboardInterrupt:
    pass
finally:
    print("Shutting down server...")
    action_server.close()
    print("Server shut down. Exiting.")
