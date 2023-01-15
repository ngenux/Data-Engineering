from kafka import KafkaConsumer
import json

# Getting the data as JSON
consumer = KafkaConsumer('data-stream',
bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('ascii')))

for message in consumer:
    price = (message.value)['data']['amount']
    print('Bitcoin price: ' + price)