import datetime
import json
import logging
import os
from io import BytesIO
from datetime import datetime, timezone

import requests as requests
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image, UnidentifiedImageError
from requests.exceptions import MissingSchema

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def start_kafka() -> None:
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
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    for msg in consumer:
        try:
            json_value = msg.value
            object_id = json_value.get('id')
            logging.info(f'Received message for id: {object_id}')
            image_uri = json_value['data']['ac:accessURI']
            data_elements = get_image_info(image_uri)
            annotations = create_annotation(data_elements, json_value)
            logging.info(f'Publishing the result: {object_id}')
            send_updated_opends(annotations, producer)
        except:
            logging.exception(f'Failed to process message: {msg}')


def create_annotation(data_elements: dict, json_value: dict):
    annotations = []
    for key, value in data_elements.items():
        if json_value.get('data').get(key) is None:
            annotations.append(
                {
                    'type': 'Annotation',
                    'motivation': 'https://hdl.handle.net/adding',
                    'creator': 'https://hdl.handle.net/enrichment-service-pid',
                    'created': timestamp_now(),
                    'target': {
                        'id': json_value.get('id'),
                        'type': 'https://hdl.handle.net/21...',
                        'indvProp': key
                    },
                    'body': {
                        'source': json_value.get('data').get('ac:accessURI'),
                        'values': value,
                    }
                })
    return annotations


def timestamp_now():
    timestamp = str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone


def send_updated_opends(annotations: list, producer: KafkaProducer) -> None:
    for annotation in annotations:
        logging.info('Publishing annotation: ' + str(annotation))
        producer.send('annotation', annotation)


def get_image_info(image_uri: str) -> dict:
    """
    Checks if the Image url works and gathers metadata information from the image
    :param image_uri: The image url from which we will gather metadata
    :return: Returns a list of additional info about the image
    """
    try:
        img = Image.open(requests.get(image_uri, stream=True).raw)
        img_file = BytesIO()
        img.save(img_file, img.format, quality='keep')
        additional_info = {'exif:PixelXDimension': img.width,
                           'exif:PixelYDimension': img.height,
                           'dcterms:format': img.format.lower(),
                           'dcterms:extent': img_file.tell() / (1000.0 * 1000.0),
                           'ac:variant': 'acvariant:v008'}
    except (FileNotFoundError, UnidentifiedImageError, MissingSchema):
        additional_info = {'ac:variant': 'acvariant:v007'}
        logging.exception('Failed to retrieve picture')
    return additional_info


if __name__ == '__main__':
    start_kafka()
    # get_image_info('https://repo.rbge.org.uk/image_server.php?kind=1500&path_base64=L2hlcmJhcml1bV9zcGVjaW1lbl9zY2Fucy9FMDAvMzE4LzI3OC81NTI3OS5qcGc=')
