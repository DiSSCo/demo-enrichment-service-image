import json
import logging
import os

import requests as requests
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def start_kafka(name: str) -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    :param name: The topic name the Kafka listener needs to listen to
    """
    consumer = KafkaConsumer(name, group_id='group', bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logging.info("Starting consumer for topic: %s", name)
    for msg in consumer:
        logging.info(msg.value)
        json_value = msg.value
        for image in json_value['ods:images']:
            image_uri = image["ods:imageURI"]
            image['additional_info'] = get_image_info(image_uri)
        logging.info("Publishing the result: %s", json_value)
        producer.send('topic', json_value)


def get_image_info(image_uri: str) -> list:
    """
    Checks if the Image url works and gathers metadata information from the image
    :param image_uri: The image url from which we will gather metadata
    :return: Returns a list of additional info about the image
    """
    try:
        img = Image.open(requests.get(image_uri, stream=True).raw)
        additional_info = {'width': img.width,
                           'height': img.height,
                           'source': 'enrichment-service-demo',
                           'format': img.format,
                           'active_url': True}
        if img.format == 'JPEG':
            add_jpeg_info(additional_info, img)
        if img.format == 'GIF':
            add_gif_info(additional_info, img)
        image_additional_info = [additional_info]
    except FileNotFoundError:
        additional_info = {'active_url': False}
        logging.exception('Failed to retrieve picture')
        image_additional_info = [additional_info]
    return image_additional_info


def add_gif_info(additional_info: dict, img: Image.Image) -> None:
    """
    If the image is a GIF retrieve GIF specific metadata
    :param additional_info: The additional_indo dict in which the metadata is stored
    :param img: The Image object from which we can gather the metadata
    """
    additional_info['gif_version'] = img.info['version']


def add_jpeg_info(additional_info: dict, img: Image.Image) -> None:
    """
    If the image is a JPEG retrieve JPEG specific metadata
    :param additional_info: The additional_indo dict in which the metadata is stored
    :param img: The Image object from which we can gather the metadata
    """
    jfif_version = {'jfif_version_major': img.info['jfif_version'][0],
                    'jfif_version_minor': img.info['jfif_version'][1]}
    additional_info['jfif_version'] = jfif_version
    dpi = {'dpi_width': img.info['dpi'][0], 'dpi_height': img.info['dpi'][1]}
    additional_info['dpi'] = dpi


if __name__ == '__main__':
    start_kafka('images')
