from confluent_kafka import avro  
from confluent_kafka.avro import AvroProducer  
  
# Define Avro schema  
value_schema_str = """  
{  
   "namespace": "my.test",  
   "name": "value",  
   "type": "record",  
   "fields" : [  
     {"name" : "name", "type" : "string"},  
     {"name" : "age",  "type" : "int"}  
   ]  
}  
"""  
value_schema = avro.loads(value_schema_str)  
  
# Define Kafka Avro Producer configuration  
conf = {'bootstrap.servers': "localhost:9092",  
        'schema.registry.url': 'http://localhost:8081'}  
  
# Create AvroProducer instance  
p = AvroProducer(conf, default_value_schema=value_schema)  
  
# Produce message  
p.produce(topic='bid_requests_topic', value={"name": "Bid1", "age": 30})  
p.flush()  
