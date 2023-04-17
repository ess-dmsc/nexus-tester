import json
import os
import socket
import time

GRAYLOG_HOST = "10.100.211.187"
GRAYLOG_PORT = 12201


def create_gelf_message(message, level, additional_fields):
    gelf_message = {
        "version": "1.1",
        "host": socket.gethostname(),
        "full_message": message,
        "timestamp": time.time(),
        "level": level,  # Error level
        "_facility": "ESS",
        "_pid": os.getpid(),
        "_process_name": os.path.basename(__file__),
    }

    for key, value in additional_fields.items():
        gelf_message[f"_{key}"] = value

    return json.dumps(gelf_message)


def send_message_to_graylog(message, level, additional_fields=None):
    if additional_fields is None:
        additional_fields = {}

    gelf_message = create_gelf_message(message, level, additional_fields)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((GRAYLOG_HOST, GRAYLOG_PORT))
        sock.sendall(gelf_message.encode() + b"\x00")


if __name__ == "__main__":
    error_message = "An error occurred while processing a NeXus file."
    additional_fields = {"filename": "example.nxs"}
    send_message_to_graylog(error_message, 3, additional_fields)