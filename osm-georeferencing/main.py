import json
import logging
import os
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    :param name: The topic name the Kafka listener needs to listen to
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'), group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        try:
            logging.info('Received message: ' + str(msg.value))
            json_value = msg.value
            specimen_data = json_value['data']
            if specimen_data.get('dwc:locality') is None:
                logging.warning('No locality information available, skipping message')
            else:
                result = run_georeference(specimen_data)
                annotations = map_to_annotation(json_value, result)
                send_updated_opends(annotations, producer)
        except:
            logging.exception('Failed to process message')


def map_to_annotation(json_value, result):
    annotation_lat = {
        'type': 'Annotation',
        'motivation': 'https://hdl.handle.net/adding',
        'creator': 'https://hdl.handle.net/enrichment-service-pid',
        'created': str(datetime.now(tz=timezone.utc).isoformat()),
        'target': {
            'id': 'https://hdl.handle.net/' + json_value['id'],
            'type': 'https://hdl.handle.net/21...',
            'invdProp': 'dwc:decimalLatitude'
        },
        'body': {
            'source': result['display_name'],
            'purpose': 'georeferencing',
            'values': str(result['lat']),
            'score': str(result['importance'])
        }
    }

    annotation_long = {
        'type': 'Annotation',
        'motivation': 'https://hdl.handle.net/adding',
        'creator': 'https://hdl.handle.net/enrichment-service-pid',
        'created': str(datetime.now(tz=timezone.utc).isoformat()),
        'target': {
            'id': 'https://hdl.handle.net/' + json_value['id'],
            'type': 'https://hdl.handle.net/21...',
            'invdProp': 'dwc:decimalLongitude'
        },
        'body': {
            'source': result['display_name'],
            'purpose': 'georeferencing',
            'values': str(result['lon']),
            'score': str(result['importance'])
        }
    }
    return [annotation_lat, annotation_long]


def send_updated_opends(annotations: list, producer: KafkaProducer) -> None:
    for annotation in annotations:
        logging.info('Publishing annotation: ' + str(annotation))
        producer.send('annotation', annotation)


def run_georeference(json_data):
    querystring = json_data['dwc:locality'] + ', ' + json_data['dwc:country']
    response = requests.get('https://nominatim.openstreetmap.org/search.php?q=' + querystring + '&format=jsonv2')
    response_json = json.loads(response.content)
    logging.info('Highest hit is: ' + json.dumps(response_json[0], indent=2))
    return response_json[0]


def run_local(example: str):
    response = requests.get(example)
    attributes = json.loads(response.content)['data']['attributes']
    data = attributes['data']
    result = run_georeference(data)
    annotations = map_to_annotation(attributes, result)
    logging.info('Created annotations: ' + str(annotations))


if __name__ == '__main__':
    # run_local('https://sandbox.dissco.tech/api/v1/specimens/20.5000.1025/E1R-NH0-J3J')
    start_kafka()
