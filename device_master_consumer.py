from confluent_kafka import Consumer, OFFSET_BEGINNING
import socket
import json
import os

def update_code(msg):
    value_dict = json.loads(msg)
    if value_dict['type'] == "code_update":
        # enter the code to run sh file
        # os.popen('sh /home/oracle/scripts/start.sh')
        pass


def consume_alerts(topic="device_master"):
    """
    @param topic : name of the topic to consume the alerts
    """
    consumer = Consumer({'bootstrap.servers': '52.66.213.65:9092',
    'client.id': socket.gethostname(), "group.id":"mygroup"})
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                print("Waiting...")
            
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            
            else:

                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

                update_code(msg.value())
    
    except KeyboardInterrupt:
        pass
    
    finally:
        consumer.close()
            
if __name__ == '__main__':
    consume_alerts()