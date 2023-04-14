import time
from threading import Timer

from confluent_kafka import Consumer, TopicPartition
from streaming_data_types import deserialise_wrdn
from test_nexus import check_nexus_file

BROKER = '10.100.1.19:9092'
TOPIC = 'ymir_filewriter'
START_TIMESTAMP_SEC = 24 * 60 * 60
POOL_SIZE = 4


def find_offset_by_timestamp(consumer, topic_partition, timestamp):
    low = consumer.get_watermark_offsets(topic_partition, timeout=1)[0]
    high = consumer.get_watermark_offsets(topic_partition, timeout=1)[1]

    while low < high:
        mid = (low + high) // 2
        consumer.seek(TopicPartition(topic_partition.topic, topic_partition.partition, mid))

        msg = consumer.poll(timeout=5)

        if msg is None:
            low = mid + 1
            continue

        if msg.timestamp()[1] < timestamp:
            low = mid + 1
        elif msg.timestamp()[1] > timestamp:
            high = mid
        else:
            return mid

    return low


def schedule_check_nexus_file(file_name, delay=10):
    time.sleep(delay)
    check_nexus_file(file_name)

def schedule_check_nexus_file(file_name, delay=10):
    timer = Timer(delay, check_nexus_file, args=(file_name,))
    timer.start()

def process_message(message):
    if message.value()[4:8] == b'wrdn':
        result = deserialise_wrdn(message.value())
        try:
            schedule_check_nexus_file(result.file_name)
        except Exception as e:
            print(f'failed to open {result.file_name}')
            print(e)


def main(broker, topic):
    conf = {
        'bootstrap.servers': broker,
        'group.id': 'nexus-file-checker',
        'auto.offset.reset': 'latest',
    }

    consumer = Consumer(conf)

    topic_partitions = [
        TopicPartition(topic, p)
        for p in consumer.list_topics(topic).topics[topic].partitions.keys()
    ]

    consumer.assign(topic_partitions)

    start_timestamp_ms = int((time.time() - START_TIMESTAMP_SEC) * 1000)
    for tp in topic_partitions:
        offset = find_offset_by_timestamp(consumer, tp, start_timestamp_ms)
        consumer.seek(TopicPartition(tp.topic, tp.partition, offset))

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
        else:
            process_message(msg)

    consumer.close()


if __name__ == "__main__":
    main(BROKER, TOPIC)