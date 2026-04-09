import lemegeton

if __name__ == "__main__":

    def pull_callback(message):
        print(message.value)

    puller = lemegeton.create_puller(
        name="test_pusher",
        callback=pull_callback,
        ip_address="localhost",
    )

    import time

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        puller.close()
