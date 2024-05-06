import uuid
import base64

def generate_uuid_string():
    return str(uuid.uuid4())

def generate_binary_uuid():
    return uuid.uuid4().bytes

def convert_uuid_string_to_binary(uuid_string):
    return uuid.UUID(uuid_string).bytes

def convert_binary_uuid_to_base64(binary_uuid):
    return base64.b64encode(binary_uuid).decode('ascii')

def convert_binary_uuid_to_string(binary_uuid):
    return str(uuid.UUID(bytes=binary_uuid))
