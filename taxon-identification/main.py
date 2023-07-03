import json
import logging
import os
import uuid
import datetime
from typing import Dict

import requests
from cloudevents.http import CloudEvent, to_structured
from kafka import KafkaConsumer, KafkaProducer


def check_taxonomy(json_value) -> dict:
    scientific_name = json_value.get('ods:authoritative').get("ods:name")
    query = {'name': scientific_name,
             'verbose': True}

    response = requests.get('https://api.gbif.org/v1/species/match2', params=query)


def start_kafka():
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'), group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=True,
                             max_poll_interval_ms=50000,
                             max_poll_records=10)
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')])

    for msg in consumer:
        try:
            json_value = msg.value
            object_id = json_value.get('ods:authoritative').get('ods:physicalSpecimenId')
            logging.info(f'Received message for id: {object_id}')
            json_value = check_taxonomy(json_value)
            logging.info(f'Publishing the result: {object_id}')
            send_updated_opends(json_value, producer)
        except:
            logging.exception(f'Failed to process message: {msg}')


def send_updated_opends(opends: dict, producer: KafkaProducer) -> None:
    attributes = {
        "id": str(uuid.uuid4()),
        "type": "eu.dissco.enrichment.response.event",
        "source": "https://dissco.eu",
        "subject": "gbif-backbone-taxonomy-name-addition",
        "time": str(datetime.datetime.now(tz=datetime.timezone.utc).isoformat()),
        "datacontenttype": "application/json"
    }
    data: Dict[str, dict] = {"openDS": opends}
    event = CloudEvent(attributes=attributes, data=data)
    headers, body = to_structured(event)
    headers_list = [(k, str.encode(v)) for k, v in headers.items()]
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), body, headers=headers_list)


if __name__ == '__main__':
    start_kafka()
