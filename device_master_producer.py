from confluent_kafka import Producer
import json
from datetime import datetime

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

def generate_global_id():
    now = datetime.now()
    date = now.strftime("%d/%m/%Y")
    time = time = now.strftime("%H:%M:%S")
    global_id = now.strftime("%Y%m%d%H%M%S")
    
    return date, time, global_id

def produce_alerts(wh_id : str, type :str, topic = "device_master"):
    """
    @param wh_id : warehouse id of the site where you want to update the code\n
    @param type : type of command you want to perform\n
    @param topic : name of the topic to publish the commands
    """
    producer = Producer({'bootstrap.servers': '52.66.213.65:9092'})
    
    date, time, id = generate_global_id()
    command = {
        "id":id,
        "date":date,
        "time" : time,
        "type":type,
        "warehouse_id" : wh_id,
    }
    
    producer.produce(topic, json.dumps(command), id, callback=delivery_callback)
    
    producer.poll(10000)
    producer.flush()

if __name__ == "__main__":
    """
    Sample to give you an idea what an alert will look like
    """
    produce_alerts(wh_id="10", type="code_update")