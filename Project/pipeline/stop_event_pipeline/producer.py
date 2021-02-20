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
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

import sys
sys.path.append("..")
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from urllib.request import urlopen
from datetime import date
from validations import *
from bs4 import BeautifulSoup

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    today = date.today()

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            # print("Produced record to topic {} partition [{}] @ offset {}"
            #      .format(msg.topic(), msg.partition(), msg.offset()))

    url = "http://rbi.ddns.net/getStopEvents"

    html = urlopen(url)

    soup = BeautifulSoup(html, 'lxml')

    table_data = []

    trip_index = []

    # date_time = today.strftime("%b_%d_%Y")
    # file_name = '/home/miyasato/nums_' + date_time + '.json'

    count = 0
    # with open(file_name, 'w') as file:
        # json.dump(len(data), file)

    for trip in soup.find_all("h3"):
        text = trip.text
        start = text.index('trip') + 5
        end = text.index('for today') - 1
        id = text[start:end]
        trip_index.append(id)

    for j, table in enumerate(soup.find_all("table")):
        fields = []
        for tr in table.find_all('tr', recursive=False):
            for th in tr.find_all('th', recursive=False):
                fields.append(th.text)

        for tr in table.find_all('tr'):
            data = {}
            for i, td in enumerate(tr.find_all('td')):
                data[fields[i]] = td.text
            if data:
                data["trip_id"] = trip_index[j]
                trip_id = validateTripId(data)
                route_id = validateRouteId(data)
                direction = validateDirection(data)
                service_key = validateServiceKey(data)

                if trip_id is False or route_id is False or direction is False or service_key is False:
                    continue

                # send record to kafka
                producer.produce(topic, value=json.dumps(data), on_delivery=acked)

                # p.poll() serves delivery reports (on_delivery)
                # from previous produce() calls.
                producer.poll(0)
                if count % 10000 == 0:
                    producer.flush()
                count+=1
                break


    producer.flush()
    print(f"Total of {count} stop event records inserted.")
    # txt = "\n{} messages were produced to topic {}!".format(delivered_records, topic)
    # with open(file_name, 'a') as file:
        # file.write(txt)
