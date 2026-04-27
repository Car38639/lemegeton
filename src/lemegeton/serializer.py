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
