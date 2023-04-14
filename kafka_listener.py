from kafka import KafkaConsumer, TopicPartition
from test_nexus import check_nexus_file
import time
from threading import Timer

from streaming_data_types import deserialise_wrdn


def find_offset_by_timestamp(consumer, topic_partition, timestamp):
    low = consumer.beginning_offsets([topic_partition])[topic_partition]
    high = consumer.end_offsets([topic_partition])[topic_partition]

    while low < high:
        mid = (low + high) // 2
        consumer.seek(topic_partition, mid)

        try:
            records = [record for records_list in consumer.poll(5).values() for record in records_list]
            if not records:
                low = mid + 1
                continue
            msg = records[0]
        except StopIteration:
            # No records returned, continue to the next iteration
            low = mid + 1
            continue

        if msg.timestamp < timestamp:
            low = mid + 1
        elif msg.timestamp > timestamp:
            high = mid
        else:
            return mid

    return low


def schedule_check_nexus_file(file_name, delay=10):
    timer = Timer(delay, check_nexus_file, args=(file_name,))
    timer.start()


def process_message(message):
    if message.value[4:8] == b'wrdn':
        result = deserialise_wrdn(message.value)
        try:
            schedule_check_nexus_file(result.file_name)
        except Exception as e:
            print(f'failed to open{result.file_name}')
            print(e)



def main(broker, topic):
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        auto_offset_reset='latest',
    )

    # Get the partitions for the topic
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        print(f"No partitions found for topic '{topic}'.")
        return

    # Assign the partitions to the consumer and set the starting offset for each partition based on the timestamp
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    start_timestamp_ms = int((time.time() - 30*24*60*60) * 1000)
    for tp in topic_partitions:
        offset = find_offset_by_timestamp(consumer, tp, start_timestamp_ms)
        consumer.seek(tp, offset)


    # Process messages from the Kafka topic
    for message in consumer:
        process_message(message)

if __name__ == "__main__":
    BROKER = '10.100.1.19:9092'
    TOPIC = 'ymir_filewriter'
    main(BROKER, TOPIC)
