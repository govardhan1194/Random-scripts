from kafka import KafkaConsumer
import json

def json_deserializer(data):
    return json.loads(decode('utf-8'))

if __name__ == "__main__":
    consumer = KafkaConsumer(
        "test",
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-a",
        value_deserializer=json_deserializer)
    print("starting the consumer")
    for msg in consumer:
        print("Registered User = {}".format(json.loads(msg.value)))

        
      
