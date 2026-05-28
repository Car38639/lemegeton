import lemegeton

context = lemegeton.Context()

import time

from lemegeton.msg.common.std_msgs_pb2 import String

action_client = lemegeton.client.ActionClient(
    context=context,
    name="test_action_server",
    goal_class=String,
    result_class=String,
    feedback_class=String,
    ip_address="192.168.1.100",
    timeout=3.0,
)


def feedback_callback(feedback):
    print("Received feedback:", feedback.value)


def result_callback(status, result):
    print(f"Received result with status {status}: {result.value}")


def cancel_callback(status, result):
    print(f"Received cancel notification with status {status}: {result.value}")


time.sleep(1)  # 等待訂閱線程啟動並連接到服務器

goal = String(value="1st Action Server!")
goal_id, future = action_client.send_goal(
    goal, feedback_callback, result_callback, cancel_callback
)

goal2 = String(value="2nd Action Server!")
goal_id2, future2 = action_client.send_goal(
    goal2, feedback_callback, result_callback, cancel_callback
)

i = 0
while not future2.done():
    i += 1
    time.sleep(1)
    if i == 3:
        action_client.cancel_goal(goal_id)

    if future.cancelled():
        print("Goal was canceled.")
    elif future.done():
        try:
            result_dict = future.result()
        except Exception as e:
            print(f"Error getting final result: {e}")

action_client.close()
print("Exiting client...")
