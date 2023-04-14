# NeXus File Tester

This project is designed to test NeXus files for compliance with specific standards, generate metrics for passed and failed tests, and push these metrics to Graphite. The project is composed of three main components:

1. `test_nexus.py`: A script that tests NeXus files for compliance and generates metrics for passed and failed tests.
2. `graphite_pusher.py`: A module that sends metrics to Graphite using a rate-limited queue.
3. `graylog_pusher.py`: A module that sends logging messages to Graylog.
4. `kafka_listener.py`: A script that listens to a Kafka topic for incoming NeXus files and processes them using `test_nexus.py`.

## Features

- Validates NeXus files against specific standards
- Generates metrics for passed and failed tests
- Sends metrics to Graphite using a rate-limited queue
- Sends test messages to Graylog
- Listens to a Kafka topic for incoming NeXus files

## Dependencies

- Python 3.6 or later
- h5py
- confluent-kafka
- streaming_data_types

## Installation

1. Clone the repository:
```
git clone https://github.com/yourusername/nexus-file-tester.git
```


2. Change to the project directory:
```
cd nexus-file-tester
```



3. Create a virtual environment and activate it:
```
python -m venv venv
source venv/bin/activate # For Unix/Linux
venv\Scripts\activate # For Windows
```


4. Install the required dependencies:
```
pip install -r requirements.txt
```


## Usage

1. Configure the `GRAPHITE_HOST`, `GRAPHITE_PORT`, `BROKER`, and `TOPIC` variables in the respective scripts (`test_nexus.py`, `graphite_pusher.py`, and `kafka_listener.py`) to match your environment.

2. Run the `kafka_listener.py` script to start listening to the specified Kafka topic for incoming NeXus files:
```
python kafka_listener.py
```


3. The listener will process incoming NeXus files using `test_nexus.py`, which in turn sends metrics to Graphite through `graphite_pusher.py`, and messages to Graylog through `graylog_pusher.py`.

