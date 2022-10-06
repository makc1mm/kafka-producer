import json
from kafka import KafkaProducer

topic = 'test_topic'

broker = 'localhost:9092'

file = 'test.json'


def main():
    with open(file, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    producer = KafkaProducer(
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        bootstrap_servers=broker
    )
    producer.send(topic, data)
    producer.close()


if __name__ == "__main__":
    main()