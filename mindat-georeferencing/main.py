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
                if result is not None:
                    annotations = map_to_annotation(json_value, result)
                    send_updated_opends(annotations, producer)
        except:
            logging.exception('Failed to process message')


def map_to_annotation(json_value, result):
    annotation_lat = {
        'type': 'Annotation',
        'motivation': 'https://hdl.handle.net/adding',
        'creator': 'https://hdl.handle.net/enrichment-service-pid',
        'created': timestamp_now(),
        'target': {
            'id': json_value['id'],
            'type': 'https://hdl.handle.net/21...',
            'indvProp': 'dwc:decimalLatitude'
        },
        'body': {
            'source': result['txt'],
            'purpose': 'georeferencing',
            'values': str(result['latitude']),
            'id': str(result['id'])
        }
    }

    annotation_long = {
        'type': 'Annotation',
        'motivation': 'https://hdl.handle.net/adding',
        'creator': 'https://hdl.handle.net/enrichment-service-pid',
        'created': timestamp_now(),
        'target': {
            'id': json_value['id'],
            'type': 'https://hdl.handle.net/21...',
            'indvProp': 'dwc:decimalLongitude'
        },
        'body': {
            'source': result['longitude'],
            'purpose': 'georeferencing',
            'values': str(result['longitude']),
            'id': str(result['id'])
        }
    }
    return [annotation_lat, annotation_long]


def timestamp_now():
    timestamp = str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone


def send_updated_opends(annotations: list, producer: KafkaProducer) -> None:
    for annotation in annotations:
        logging.info('Publishing annotation: ' + str(annotation))
        producer.send('annotation', annotation)


def run_georeference(json_data):
    querystring = json_data['dwc:locality']
    response = requests.get('https://api.mindat.org/localities/?txt=' + querystring,
                          headers={'Authorization': 'Token ' + os.environ.get('API_KEY')})
    response_json = json.loads(response.content)
    if len(response_json) == 0:
        logging.info("No results for this locality where found: " + querystring)
    else:
        logging.info('Highest hit is: ' + json.dumps(response_json.get('results')[0], indent=2))
        return response_json.get('results')[0]


def run_local(example: str):
    response = requests.get(example)
    attributes = json.loads(response.content)['data']['attributes']
    data = attributes['data']
    result = run_georeference(data)
    annotations = map_to_annotation(attributes, result)
    logging.info('Created annotations: ' + str(annotations))


if __name__ == '__main__':
    # run_local('https://sandbox.dissco.tech/api/v1/specimens/20.5000.1025/QU7-7B5-R9K')
    start_kafka()
