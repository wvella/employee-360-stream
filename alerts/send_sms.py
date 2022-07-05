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
import argparse
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from twilio.rest import Client


class SMSEvent(object):
    """
    SMS Event Record
    Args:
        body (str): Body of the SMS text message
    """
    def __init__(self, body):
        self.body = body

def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    if obj is None:
        return None

    return SMSEvent(body=obj['ALERT'])

def load_avro_schema_from_sr(url, authinfo, subjectname):
    sr = SchemaRegistryClient({'url': url,'basic.auth.user.info': authinfo})
    schema = sr.get_latest_version(subjectname)
    schema = sr.get_schema(schema.schema_id)
    value_schema = schema.schema_str

    return value_schema

def main(args):
    topic = args['topic']

    schema_registry_conf = {'url': args['schema.registry.url'],'basic.auth.user.info': args['basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_schema = load_avro_schema_from_sr(args['schema.registry.url'], args['basic.auth.user.info'],args['schema.subjectname'])
    avro_deserializer = AvroDeserializer(schema_registry_client,value_schema,dict_to_user)
    string_deserializer = StringDeserializer('utf_8')
    
    consumer_conf = {'bootstrap.servers': args['bootstrap.servers'],
                     'security.protocol': args['security.protocol'],
                     'sasl.mechanism': args['sasl.mechanisms'],
                     'sasl.username': args['sasl.username'],
                     'sasl.password': args['sasl.password'],
                     'group.id': args['group.id'],
                     'auto.offset.reset': args['auto.offset.reset'],
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            smseventconsumed = msg.value()
            if smseventconsumed is not None:
                print("Message Key [{}] successfully consumed on Topic: [{}] on Partition: [{}] at Offset: [{}]\n" 
                      "\tBody: {}\n"
                      .format(msg.key(),msg.topic(),msg.partition(),msg.offset(), smseventconsumed.body))
                client = Client(account_sid, auth_token)

                if args['sms.send'] == 'true':
                    phone_numbers = ["<<phone1>>","<<phone2>>"]
                    for number in phone_numbers:
                        message = client.messages.create(
                        to=number,
                        from_="<<fromphone1>>",
                        body=smseventconsumed.body)
                        print(message.sid)
                        time.sleep(20)

        except KeyboardInterrupt:
            break

    consumer.close()

if __name__ == '__main__':
    # Parse the command line.
    parser = argparse.ArgumentParser(description="SMS Alert Serialzing Producer")
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Your Account SID from twilio.com/console
    account_sid = "<<accountsid>>"
    # Your Auth Token from twilio.com/console
    auth_token  = "<<authtoken>>"

    main(config)