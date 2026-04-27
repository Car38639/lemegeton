from lemegeton.gateway import Gateway

if __name__ == "__main__":
    gateway = Gateway()
try:
    gateway.run()
except KeyboardInterrupt:
    print("\n正在關閉 Gateway...")
finally:
    pass
