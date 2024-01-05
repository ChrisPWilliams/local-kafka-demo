from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    group_id="login_consumer",
    consumer_timeout_ms=1000
    )
consumer.subscribe(["data_engineers", "software_engineers"])

for message in consumer:
    print(message.value)

"""consumer is an Iterator object that returns messages from the topic, it doesn't know when the topic is 
no longer going to receive messages, so we need to end the process manually with keyboard interrupt (CTRL-C)"""
consumer.commit()
consumer.close()