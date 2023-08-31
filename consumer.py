from kafka import KafkaConsumer
from json import loads
import time

consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['3.35.168.158:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group',
    # value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=1000
)

# for message in consumer:
#     print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % (
#         message.topic, message.partition, message.offset, message.key, message.value
#     ))


while 1:
    for message in consumer:
        print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % (
            message.topic, message.partition, message.offset, message.key, message.value
        ))

    time.sleep(1)
