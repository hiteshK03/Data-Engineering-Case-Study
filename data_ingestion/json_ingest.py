from confluent_kafka import Producer  
import json  
  
# Define Kafka configuration  
conf = {'bootstrap.servers': "localhost:9092"}  
  
# Create Producer instance  
p = Producer(**conf)  
  
def delivery_report(err, msg):  
    """ Called once for each message produced to indicate delivery result.  
        Triggered by poll() or flush(). """  
    if err is not None:  
        print('Message delivery failed: {}'.format(err))  
    else:  
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))  
  
# Read data from JSON  
with open('ad_impressions.json', 'r') as f:  
    data = json.load(f)  
  
# Produce message  
for record in data:  
    p.produce('ad_impressions_topic', json.dumps(record), callback=delivery_report)  
  
# Wait for any outstanding messages to be delivered and delivery reports to be received.  
p.flush()  