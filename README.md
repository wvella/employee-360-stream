# employee-360-stream
Public repo for a rough demo of employee 360 using Kafka and KSQLDB.

This demo is a very rough demo of building a pipeline joining three seperate datasets together to form a Customer 360 view using Kafka, Connect and KSQLDB.

￼

The datastreams are built on the following:

1. Python Application - Simulating door swipes from an ID access card
2. PostgreSQL - Stores employe information in a relational database
3. MongoDB - Stores each record when a tool is borrowed
4. Optional SMS alerting using Twilio

This demo hasn't been matured to automatically deploy the application in a demo environment, nor does it come with any warranty. The RAW topic AVRO schema files have been provided to describe the structure of the Source events.

To get the demo up and running, you will need to do the following:

1. Create a Confluent Cloud of Confluent Platform environment (Kafka, Schema Registry, Connect and KSQLDB)
2. Create the Service Accounts, API Keys and ACLs
3. Update the configuration files
4. If SMS alerting is required, update the send_sms.py application with the correct phone numbers
5. Create the PostgreSQL and MongoDB databases
6. Create the Connectors to PostgreSQL and MongoDB
7. Create the kSQLDB apps