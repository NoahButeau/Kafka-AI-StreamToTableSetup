from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.admin import AdminClient   
import openai
import json
import sqlite3
import mysql.connector
from confluent_kafka import Consumer, KafkaException
from config import config, mySQL_config, sr_config
import re

example = 'CREATE TABLE Temperature (city VARCHAR(255), reading DECIMAL(5,2), unit VARCHAR(255), timestamp BIGINT);'
def set_consumer_configs(topic):
    config['group.id'] = topic
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False


def insertDataIntoDatabase(databaseName,data, cursor, name,conn):
    columns = ', '.join(data.keys())
    values = tuple(data.values())
    sql = f"INSERT INTO {databaseName}.{name} ({columns}) VALUES {values}"
    print(sql)
    try:
        cursor.execute(sql)
        conn.commit() 
        print("Inserted")
    except Exception as e:
        print(f"Error inserting data: {e}")


def openAI():
    schema_registry_client = SchemaRegistryClient(sr_config)
    subjects = schema_registry_client.get_subjects()
    print(subjects)
    subject = input("Now that we have created a database \nWhat subject would you like to choose ")
    schemaID = schema_registry_client.get_latest_version(subject).schema_id

    schema = schema_registry_client.get_schema(schemaID).schema_str
    openai.api_key = '<openAI api key>'
    message = openai.Completion.create(
        engine='text-davinci-002',
        prompt=f"Example you can use for the following is {example} Can you return just a query to make a database based on this, please return the query:{schema}",
        max_tokens=1024,
        n=1,
        stop=None,
        temperature=0.5
    )
    messageJson = json.dumps(message)
    messageDict = json.loads(messageJson)
    return messageDict['choices'][0]['text']


def list_topics():
    admin_client = AdminClient(config)
    metadata = admin_client.list_topics(timeout=10)
    topics = metadata.topics

    for topic in topics:
        print(topic)



x = input("Would you like to create a new database or open a stream (y/n) ")
if x == "y":
    name = input("What would you like the databae name ")
    conn = mysql.connector.connect(**mySQL_config)
    print('connected successfully')
    cursor = conn.cursor()
    cursor.execute(f'CREATE DATABASE {name}')
    print("Create database successfully")
    conn = mysql.connector.connect(**mySQL_config, database=f'{name}')
    cursor = conn.cursor()
    query = openAI()
    tableNameStr = re.search(r"CREATE TABLE (\w+)", query)
    if tableNameStr:
        tableName = tableNameStr.group(1)
    else:    
        tableNameStr = re.search(r"CREATE TABLE (\w+)", query)
        tableName = tableNameStr.group(1)
    
    cursor.execute(query)
else:
    name = input("What is the database you want to use ")
    tableName = input("What is the table Name ")

conn = mysql.connector.connect(**mySQL_config, database=f'{name}')
cursor = conn.cursor()
list_topics()
topic = input("What topic would you like to choose ")


#topic = 'temp_readings'
set_consumer_configs(topic)
consumer = Consumer(config)
consumer.subscribe([topic])
print('Topic Subscribed to successfully')
print('Table added Successfully\nInitiate your producer to put something in the database')

try:
    while True:
        events = consumer.consume(num_messages=10, timeout=1.0)  # Consume multiple messages at once
        for event in events:
            if event is None:
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf-8', errors='replace')
                partition = event.partition()
                consumer.commit(event)
                index = val.find('{')
                data_str = val[index:]
                print(f'Received: {data_str}')
                data = json.loads(data_str)
                insertDataIntoDatabase(name,data, cursor, tableName,conn)
except KeyboardInterrupt:
    print('Canceled by user.')
finally:
    consumer.close()
