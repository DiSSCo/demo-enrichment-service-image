import datetime
import json
import logging
import os
import uuid
from typing import Dict

import requests as requests
from PIL.TiffImagePlugin import IFDRational
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image, UnidentifiedImageError
from requests.exceptions import MissingSchema
from cloudevents.http import CloudEvent, to_structured

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
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')])

    for msg in consumer:
        try:
            json_value = msg.value
            object_id = json_value.get('ods:authoritative').get('ods:physicalSpecimenId')
            logging.info(f'Received message for id: {object_id}')
            for image in json_value['ods:images']:
                image_uri = image.get("ods:imageURI")
                image['additional_info'] = get_image_info(image_uri)
            logging.info(f'Publishing the result: {object_id}')
            send_updated_opends(json_value, producer)
        except:
            logging.exception(f'Failed to process message: {msg}')


def send_updated_opends(opends: dict, producer: KafkaProducer) -> None:
    attributes = {
        "id": str(uuid.uuid4()),
        "type": "eu.dissco.enrichment.response.event",
        "source": "https://dissco.eu",
        "subject": "image-metadata-addition",
        "time": str(datetime.datetime.now(tz=datetime.timezone.utc).isoformat()),
        "datacontenttype": "application/json"
    }
    data: Dict[str, dict] = {"openDS": opends}
    event = CloudEvent(attributes=attributes, data=data)
    headers, body = to_structured(event)
    headers_list = [(k, str.encode(v)) for k, v in headers.items()]
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), body, headers=headers_list)


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
        if img.format_description is not None:
            additional_info['format_description'] = img.format_description
        if img.format == 'JPEG':
            add_jpeg_info(additional_info, img)
        if img.format == 'GIF':
            add_gif_info(additional_info, img)
        image_additional_info = [additional_info]
    except (FileNotFoundError, UnidentifiedImageError, MissingSchema):
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
    additional_info['gif_version'] = img.info.get('version')


def add_jpeg_info(additional_info: dict, img: Image.Image) -> None:
    """
    If the image is a JPEG retrieve JPEG specific metadata
    :param additional_info: The additional_indo dict in which the metadata is stored
    :param img: The Image object from which we can gather the metadata
    """
    if img.info.get('jfif_version') is not None:
        jfif_version = {'jfif_version_major': img.info.get('jfif_version')[0],
                        'jfif_version_minor': img.info.get('jfif_version')[1]}
        additional_info['jfif_version'] = jfif_version
    if img.info.get('dpi') is not None:
        dpi = {}
        if isinstance(img.info.get('dpi')[0], IFDRational):
            dpi['dpi_width'] = img.info.get('dpi')[0].numerator
        else:
            dpi['dpi_width'] = img.info.get('dpi')[0]
        if isinstance(img.info.get('dpi')[1], IFDRational):
            dpi['dpi_height'] = img.info.get('dpi')[1].numerator
        else:
            dpi['dpi_height'] = img.info.get('dpi')[1]
        additional_info['dpi'] = dpi
    if img.info.get('adobe') is not None:
        additional_info['adobe'] = True


if __name__ == '__main__':
    start_kafka()
