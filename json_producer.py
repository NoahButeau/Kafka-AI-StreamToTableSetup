from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from config import config, sr_config
import time
###############################


# This file is used for testing purposes


################################

'''class Temperature(object):
    def __init__(self, city, reading, unit, timestamp):
        self.city = city
        self.reading = reading
        self.unit = unit
        self.timestamp = timestamp
        
def temp_to_dict(temp, ctx):
    return {"city":temp.city, 
        "reading":temp.reading,
        "unit":temp.unit, 
        "timestamp":temp.timestamp}

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')
schema_str = """{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Temperature",
    "description": "Temperature sensor reading",
    "type": "object",
    "properties": {
      "city": {
        "description": "City name",
        "type": "string"
      },
      "reading": {
        "description": "Current temperature reading",
        "type": "number"
      },
      "unit": {
        "description": "Temperature unit (C/F)",
        "type": "string"
      },
      "timestamp": {
        "description": "Time of reading in ms since epoch",
        "type": "number"
      }
    }
  }"""
  
data = [Temperature('London', 12, 'C', round(time.time()*1000)),
        Temperature('Chicago', 63, 'F', round(time.time()*1000)),
        Temperature('Berlin', 14, 'C', round(time.time()*1000)),
        Temperature('Madrid', 18, 'C', round(time.time()*1000)),
        Temperature('Phoenix', 78, 'F', round(time.time()*1000))]

if __name__ == '__main__':
    topic = 'temp_readings'
    schema_registry_client = SchemaRegistryClient(sr_config)

    json_serializer = JSONSerializer(schema_str,
                                     schema_registry_client,
                                     temp_to_dict)

    producer = Producer(config)
    for temp in data:
        producer.produce(topic=topic, key=str(temp.city),
                         value=json_serializer(temp, 
                         SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

    producer.flush()'''

class Person(object):
    def __init__(self, name, age, email, address):
        self.name = name
        self.age = age
        self.email = email
        self.address = address

def person_to_dict(person, ctx):
    return {
        "name": person.name,
        "age": person.age,
        "email": person.email,
        "address": person.address
    }

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on person {event.key().decode("utf8")}: {err}')
    else:
        print(f'Person {event.key().decode("utf8")} produced to {event.topic()}')

schema_str = """{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Person",
    "description": "Person's information",
    "type": "object",
    "properties": {
      "name": {
        "description": "Person's name",
        "type": "string"
      },
      "age": {
        "description": "Person's age",
        "type": "integer"
      },
      "email": {
        "description": "Person's email address",
        "type": "string",
        "format": "email"
      },
      "address": {
        "description": "Person's address",
        "type": "string"
      }
    }
  }"""

data = [
    Person('John Doe', 30, 'johndoe@example.com', '123 Main St'),
    Person('Jane Smith', 25, 'janesmith@example.com', '456 Elm St'),
    Person('Michael Johnson', 40, 'michaeljohnson@example.com', '789 Oak St'),
    Person('Emily Davis', 35, 'emilydavis@example.com', '321 Pine St'),
    Person('David Wilson', 28, 'davidwilson@example.com', '654 Maple St')
]

if __name__ == '__main__':
    topic = 'personalInfo'
    schema_registry_client = SchemaRegistryClient(sr_config)

    json_serializer = JSONSerializer(schema_str,
                                     schema_registry_client,
                                     person_to_dict)

    producer = Producer(config)
    for person in data:
        producer.produce(topic=topic, key=person.name,
                         value=json_serializer(person, 
                         SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

    producer.flush()