import csv  
from confluent_kafka import Producer  
  
# Define Kafka configuration  
conf = {'bootstrap.servers': "localhost:9092"}  
  
# Create Producer instance  
p = Producer(**conf)  
  
def delivery_report(err, msg):  
    if err is not None:  
        print('Message delivery failed: {}'.format(err))  
    else:  
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))  
  
# Read data from CSV  
with open('clicks_and_conversions.csv', 'r') as f:  
    csv_data = csv.reader(f)  
    headers = next(csv_data)  
    for row in csv_data:  
        record = dict(zip(headers, row))  
        p.produce('clicks_and_conversions_topic', json.dumps(record), callback=delivery_report)  
  
# Wait for any outstanding messages to be delivered and reports to be received.  
p.flush()   