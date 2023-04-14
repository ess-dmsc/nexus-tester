import socket
import time
import threading
import queue

class GraphiteQueue:
    def __init__(self, host, port, interval=1):
        self.host = host
        self.port = port
        self.interval = interval
        self.metrics_queue = queue.Queue()
        self.thread = threading.Thread(target=self.process_queue)
        self.thread.daemon = True
        self.thread.start()

    def process_queue(self):
        while True:
            metric, value, timestamp = self.metrics_queue.get()
            send_metric_to_graphite(self.host, self.port, metric, value, timestamp)
            time.sleep(self.interval)

    def add_metric(self, metric, value, timestamp=None):
        self.metrics_queue.put((metric, value, timestamp))


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