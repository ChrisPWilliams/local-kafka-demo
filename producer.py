from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:29092")   # "bootstrap_servers" is the address of our Kafka cluster nodes

# Suppose we want to track logins of software & data engineers to a system. First we generate messages to appropriate topics to track logins
# 10 logins of each kind

for i in range(10):
    print(f"sending {i}")
    message_bytes = f"{i} logged in".encode('utf-8')            # need to encode message string as binary
    producer.send(topic="data_engineers", value=message_bytes)  # send message to data_engineers topic
for i in range(10):
    print(f"sending {i}")
    message_bytes = f"{i} logged in".encode('utf-8')
    producer.send(topic="software_engineers", value=message_bytes)

# Kafka bundles messages into packets for improved network throughput, so it does not always send messages immediately 
producer.flush()            # Call producer.flush to ensure all messages are sent before ending the script                                                
producer.close()