def decode_payload(payload, message_class, unpack_blobs: bool = False):
    """把收到的位元組還原成 callback 的參數。

    回傳 tuple 以便 ``callback(*args)``：``unpack_blobs=False`` 時是
    ``(message,)``，``True`` 時是 ``(message, arrays)``。後者不論發端選了
    內嵌還是搬出訊息體都一樣，收端不必分辨。
    """
    if unpack_blobs:
        from lemegeton import blob

        return blob.decode(payload, message_class)
    return (ProtobufMessageHandler.deserialize(message_class, payload),)


class ProtobufMessageHandler:
    """
    ProtobufMessageHandler handles serialization and deserialization of protobuf messages.
    """

    @staticmethod
    def serialize(message) -> bytes:
        return message.SerializeToString()

    @staticmethod
    def deserialize(message_class, message_bytes: bytes):
        message = message_class()
        message.ParseFromString(message_bytes)
        return message
