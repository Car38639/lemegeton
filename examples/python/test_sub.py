import lemegeton

if __name__ == "__main__":

    def callback(message):
        print(f"Received message: {message.value}")

    subscriber = lemegeton.create_subscriber(
        name="test_pub", ip_address="192.168.1.100", callback=callback
    )
    try:
        while True:
            pass
    except KeyboardInterrupt:
        subscriber.close()
