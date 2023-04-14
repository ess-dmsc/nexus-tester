import socket
import time
from functools import wraps


def rate_limited(min_interval):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not hasattr(wrapper, '_last_called'):
                wrapper._last_called = 0

            elapsed_time = time.time() - wrapper._last_called
            if elapsed_time < min_interval:
                time.sleep(min_interval - elapsed_time)

            result = func(*args, **kwargs)
            wrapper._last_called = time.time()
            return result

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