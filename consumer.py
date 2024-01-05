from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers=["localhost:29092"],  # Set cluster address
    auto_offset_reset="earliest",           # If there is an offset error, go to the earliest uncommitted offset
    group_id="login_consumer",              # Set a consumer group name in order for the Kafka cluster to track the last consumed offset
    consumer_timeout_ms=1000                # Shut down the consumer if there are no unconsumed messages in the topic for more than one second
    )
consumer.subscribe(["data_engineers", "software_engineers"])  # Subscribe to our topics

for message in consumer:                    # Note that "consumer" is an Iterator that will stay open until we shut down the consumer
    print(message.value)                    # Print all message values for each message in the topic. 

consumer.commit()                           # Before shutting down the consumer, ensure offsets are committed to the cluster, so that the
consumer.close()                            # consumer can continue from where it left off if new messages are produced