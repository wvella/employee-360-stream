#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This is a simple example of the SerializingProducer using Avro.
#
import sys
import time
from random import choice
import argparse
from sqlite3 import Timestamp
from datetime import datetime

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

class SwipeEvent(object):
    """
    Swipe Event Record
    Args:
        id (int): User's id
        location (str): Location of the swipe
        timestamp (date): Timestamp of the event
    """
    def __init__(self, id, location, timestamp):
        self.id = int(id)
        self.location = location
        self.timestamp = timestamp

def user_to_dict(swipeevent, ctx):
    """
    Returns a dict representation of a SwipeEvent instance for serialization.
    Args:
        swipeevent (SwipeEvent): Swipe Event instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    return dict(id=swipeevent.id,
                location=swipeevent.location,
                timestamp=swipeevent.timestamp)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print("Message Key: [{}] successfully produced to Topic: [{}] on Partition: [{}] at Offset: [{}]".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def load_avro_schema_from_sr(url, authinfo, subjectname):
    sr = SchemaRegistryClient({'url': url, 'basic.auth.user.info': authinfo })
    schema = sr.get_latest_version(subjectname)
    schema = sr.get_schema(schema.schema_id)
    value_schema = schema.schema_str
    return value_schema

def main(args):
    topic = args['topic']
    schema_registry_conf = {'url': args['schema.registry.url'],'basic.auth.user.info': args['basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_schema = load_avro_schema_from_sr(args['schema.registry.url'], args['basic.auth.user.info'],args['schema.subjectname'])
    avro_serializer = AvroSerializer(schema_registry_client, value_schema, user_to_dict)

    producer_conf = {'bootstrap.servers': args['bootstrap.servers'],
                     'security.protocol': args['security.protocol'],
                     'sasl.mechanism': args['sasl.mechanisms'],
                     'sasl.username': args['sasl.username'],
                     'sasl.password': args['sasl.password'],
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)
    print("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        try:
            event_id = [100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,126,145,146,176,177,178,179,192,193,200,201,202,203,204,205,206]
            event_location = ['door-1', 'door-2', 'door-3', 'door-4', 'door-5']
            if args['sms.send'] == 'true':
                event_location.append('door-6')
            dateTimeObj = datetime.now()
            event_timestamp = str(dateTimeObj.strftime("%d-%b-%Y %H:%M:%S.%f"))
            
            count = 0
            for _ in range(1):
                newswipe = SwipeEvent (id=choice(event_id), location=choice(event_location), timestamp=event_timestamp)
                count += 1
                producer.produce(topic=topic, key=str(newswipe.id), value=newswipe,on_delivery=delivery_report)
            producer.poll(10000)
            producer.flush()
            time.sleep(1)

        except KeyboardInterrupt:
            break

if __name__ == '__main__':
    # Parse the command line.
    parser = argparse.ArgumentParser(description="ID Cards Serialzing Producer")
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    main(config)