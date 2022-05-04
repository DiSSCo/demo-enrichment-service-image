import datetime
import json
import logging
import os
import uuid
from typing import Dict

import requests
from cloudevents.http import CloudEvent, to_structured
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def add_annotation(opends: Dict) -> Dict:
    rec_id = opends.get('ods:authoritative').get('ods:physicalSpecimenId')
    if rec_id != 'RMNH.RENA.7935':
        logging.warning('Service only usable with specimen id: RMNH.RENA.7935')
        return opends
    opends = add_record_by_info(opends)
    opends = add_publication_info(opends)
    opends = add_iiif_information(opends)
    return opends


def create_iiif_annotation(resources_iif):
    return {'source': 'publication-annotation-demo-dariah',
            'ods:annotation': resources_iif}


def add_iiif_information(opends: Dict) -> Dict:
    iiifanno = "https://jqz7t23pp9.execute-api.us-east-1.amazonaws.com/dev/" \
               "annotation/c55001e09e2b11ec887c43af9948e236/annotations.json"
    iiifannoreq = requests.get(iiifanno)
    iiifannoreqres = json.loads(iiifannoreq.content)
    resoures_iif = iiifannoreqres['resources'][1]
    opends.get('ods:unmapped').get('ods:annotations').append(create_iiif_annotation(resoures_iif))
    return opends


def create_publication_annotation() -> dict:
    return {'source': 'publication-annotation-demo-dariah',
            'ods:publications': [
                {
                    "title": "Verhandelingen over de natuurlijke geschiedenis der Nederlandsche "
                             "overzeesche bezittingen",
                    "doi": "https://doi.org/10.5962/bhl.title.114730"
                },
                {
                    "title": "Overview of the traditional Indonesian knowledge on the use of reptiles",
                    "doi": "https://doi.org/10.1088/1755-1315/716/1/012066"
                },
                {
                    "title": "Over krokodillenvangers en schedeltentoonstellingen",
                    "urn": "urn:nbn:nl:ui:28-61f80709-29fc-418b-a3cc-b96056992d10"
                },
                {
                    "title": "Three’s Company: discovery of a third syntype of Stegonotus lividus, a species of "
                             "colubrid snake from Pulau Semau, Lesser Sunda Islands, Indonesia, with comments on an "
                             "unpublished 19th Century manuscript by the naturalist Salomon Müller",
                    "doi": "https://doi.org/10.11646/zootaxa.5039.1.2"
                },
                {
                    "title": "Science on the edge of empire: E. A. Forsten (1811–1843) and the "
                             "Natural History Committee (1820–1850) in the Netherlands Indies",
                    "doi": "https://doi.org/10.1111/1600-0498.12346"
                }
            ]}


def add_publication_info(opends: Dict) -> Dict:
    opends.get('ods:unmapped').get('ods:annotations').append(create_publication_annotation())
    logging.info('Added publication information to object')
    return opends


def add_record_by_info(opends: Dict) -> Dict:
    myanno = "https://hypothes.is/api/search?uri=https://nsidr.org/#objects/20.5000.1025/1036a7dabe249de9b0cd"
    annoreq = requests.get(myanno)
    annojsonres = json.loads(annoreq.content)
    annotations = annojsonres['rows'][2]['text']
    if opends.get('ods:unmapped').get('ods:annotations') is None:
        opends['ods:unmapped']['ods:annotations'] = []
    opends.get('ods:unmapped').get('ods:annotations').append(create_annotation_recorded_by(annotations))
    logging.info('Added recorded by information to object')
    return opends


def create_annotation_recorded_by(annotations: str) -> dict:
    return {'source': 'recorded-by-annotation-demo-dariah',
            'ods:annotation': annotations}


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
            opends = msg.value
            object_id = opends.get('ods:authoritative').get('ods:physicalSpecimenId')
            logging.info(f'Received message for id: {object_id}')
            opends = add_annotation(opends)
            logging.info(f'Publishing the result: {object_id}')
            send_updated_opends(opends, producer)
        except:
            logging.exception(f'Failed to process message: {msg}')


def send_updated_opends(opends: dict, producer: KafkaProducer) -> None:
    attributes = {
        "id": str(uuid.uuid4()),
        "type": "eu.dissco.translator.event",
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
