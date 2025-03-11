import uuid
import base64
import os
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
    
    @staticmethod
    def generate_uuid_list(n):
        # Each UUID is 16 bytes.
        # os.urandom(...) is cryptographically secure in Python 2.7.
        random_bytes = os.urandom(16 * n)
        return [
            str(uuid.UUID(bytes=random_bytes[i*16:(i+1)*16])) 
            for i in range(n)
        ]