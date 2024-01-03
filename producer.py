from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:29092")

for i in range(10):
    print(f"sending {i}")
    message_bytes = f"{i} logged in".encode('utf-8')
    producer.send(topic="data_engineers", value=message_bytes)
for i in range(10):
    print(f"sending {i}")
    message_bytes = f"{i} logged in".encode('utf-8')
    producer.send(topic="software_engineers", value=message_bytes)

producer.flush()
producer.close()