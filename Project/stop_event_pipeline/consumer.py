#!/usr/bin/env python
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

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from confluent_kafka import Consumer
import json
import ccloud_lib
import datetime
import psycopg2


DBConfig = "/home/miyasato/dataeng-w21/Project/db.config"
TABLE_TRIP = "Trip"

def read_db_config(config_file):
    """Read db configurations"""

    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0:
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()

    return conf

# connect to the database
def dbconnect(conf):
    connection = psycopg2.connect(
        host=conf['DBhost'],
        database=conf['DBname'],
        user=conf['DBuser'],
        password=conf['DBpwd'],
        )
    connection.autocommit = True
    return connection

#update trip record in the database
def update_trip(conn, trip_id, route_id, service_key, direction):
    sql = f""" UPDATE {TABLE_TRIP}
               SET route_id = %s,
                   service_key = %s,
                   direction = %s
               WHERE trip_id = %s"""

    with conn.cursor() as cur:
        try:
            cur.execute(sql, (route_id, service_key, direction, trip_id))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            pass


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    db_conf = read_db_config(DBConfig)
    conn = dbconnect(db_conf)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'sensor_readings_consumer',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    # total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_value = msg.value()
                data = json.loads(record_value)
                service_key = data['service_key']
                trip_id = data['trip_id']
                direction = data['direction']
                route_id = data['route_number']
                update_trip(conn, trip_id, route_id, service_key, direction)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

