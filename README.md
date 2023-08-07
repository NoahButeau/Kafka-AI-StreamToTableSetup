# Kafka Stream to MySQL Database

This project demonstrates how to consume data from a Kafka topic and insert it into a MySQL database using Python. The code utilizes the `confluent_kafka` library to consume Kafka messages and the `mysql.connector` library to interact with the MySQL database.

## Prerequisites

- Python 3.x
- `confluent_kafka` library (`pip install confluent-kafka`)
- `mysql-connector-python` library (`pip install mysql-connector-python`)

## Setup

1. Install the required libraries by running the following commands:
pip install confluent-kafka
pip install mysql-connector-python

2. Configure the Kafka and MySQL connections:

- Open the `config.py` file and provide the necessary configurations for Kafka (`config`) and MySQL (`mySQL_config`).
- Ensure that the Kafka broker and Schema Registry configurations are correctly defined in `config.py`.
- Specify the MySQL database name and table name in the code (`name` and `tableName` variables).

## Usage

1. Run the script using the command: `python tableTool.py`.
2. You will be prompted to choose between creating a new database or opening a stream.
   - If you choose to create a new database:
     - Enter the desired database name.
     - The script will create the database and generate a SQL query based on an example schema using OpenAI's GPT-3.
     - Review and confirm the generated SQL query to create the table.
   - If you choose to open a stream:
     - Enter the existing database and table names.
     - Choose the Kafka topic you want to consume from.
3. The script will subscribe to the chosen Kafka topic, consume messages, and insert the data into the MySQL database.
4. Initiate a Kafka producer to send data to the chosen topic.
5. The script will continuously consume messages until interrupted (Ctrl+C).

Note: Make sure to have a running Kafka cluster, Schema Registry, and a MySQL database before running the script.
##learn More
https://developer.confluent.io/courses/kafka-connect/how-to-generate-data-hands-on/
These videos are from the confluent website and portions of the json_prodocer.py code are taken from there and the tutorial.
The json_prodcuer.py is only used to test.
