import socket
import time
from functools import wraps


def rate_limited(interval):
    def decorator(fn):
        last_called = [0]
        @wraps(fn)
        def wrapper(*args, **kwargs):
            elapsed_time = time.time() - last_called[0]
            time_to_wait = interval - elapsed_time
            if time_to_wait > 0:
                time.sleep(time_to_wait)
            ret = fn(*args, **kwargs)
            last_called[0] = time.time()
            return ret

        return wrapper

    return decorator


@rate_limited(5)
def send_metric_to_graphite(host, port, metric, value, timestamp=None):
    """
    Send a metric to a Graphite server.

    Args:
        host (str): The Graphite server hostname or IP address.
        port (int): The Graphite server port.
        metric (str): The metric name (e.g., "myapp.temperature").
        value (float): The metric value.
        timestamp (int, optional): The metric timestamp in seconds since the epoch.
            Defaults to the current time.
    """
    if timestamp is None:
        timestamp = int(time.time())

    message = f"{metric} {value} {timestamp}\n"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.sendall(message.encode())

if __name__ == "__main__":
    GRAPHITE_HOST = "10.100.211.201"
    GRAPHITE_PORT = 2003  # Default plaintext protocol port for Graphite

    # Replace the following values with your desired metric name and value
    METRIC_NAME = "nexus_tester.failed_tests"
    METRIC_VALUE = 20

    send_metric_to_graphite(GRAPHITE_HOST, GRAPHITE_PORT, METRIC_NAME, METRIC_VALUE)