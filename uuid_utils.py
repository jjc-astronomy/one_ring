import uuid
import base64

class UUIDUtility:
    @staticmethod
    def generate_uuid_string():
        return str(uuid.uuid4())

    @staticmethod
    def generate_binary_uuid():
        return uuid.uuid4().bytes

    @staticmethod
    def convert_uuid_string_to_binary(uuid_string):
        return uuid.UUID(uuid_string).bytes

    @staticmethod
    def convert_binary_uuid_to_base64(binary_uuid):
        return base64.b64encode(binary_uuid).decode('ascii')

    @staticmethod
    def convert_binary_uuid_to_string(binary_uuid):
        # Ensure binary_uuid is of type bytes
        if isinstance(binary_uuid, bytearray):
            binary_uuid = bytes(binary_uuid)
        return str(uuid.UUID(bytes=binary_uuid))