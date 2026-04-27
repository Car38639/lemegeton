from lemegeton.gateway import Gateway

if __name__ == "__main__":
    gateway = Gateway()
    print("==================  Gateway Has Started  ==================")

    try:
        gateway.run()
    except KeyboardInterrupt:
        print("\n正在關閉 Gateway...")

    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        print("==================  Gateway Has Been Shutdown  ==================")
