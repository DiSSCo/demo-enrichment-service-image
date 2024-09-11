import json
import logging
import os
from io import BytesIO
import uuid

import requests as requests
from typing import Dict, List, Tuple, Any
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image, UnidentifiedImageError
from requests.exceptions import MissingSchema

import shared

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
CODE_BASE = 'https://pypi.org/project/pillow/'
DCTERMS_FORMAT = 'dcterms:format'


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'),
                             group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[
                                 os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(
                                 m.decode('utf-8')),
                             enable_auto_commit=True,
                             max_poll_interval_ms=50000,
                             max_poll_records=10)
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
        value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    for msg in consumer:
        json_value = msg.value
        shared.mark_job_as_running(json_value.get('jobId'))
        image_uri = json_value.get('object').get('ac:accessURI')
        timestamp = shared.timestamp_now()
        image_assertions, additional_info = get_image_measurements(image_uri,
                                                                   timestamp)
        annotations = create_annotation(image_assertions, additional_info,
                                        json_value.get('object'), timestamp)
        publish_annotation_event(
            map_to_annotation_event(annotations, json_value.get('jobId')),
            producer)


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    media = json.loads(response.content).get('data').get('attributes')
    image_uri = media.get('ac:accessURI')
    timestamp = shared.timestamp_now()
    image_assertions, additional_info = get_image_measurements(image_uri,
                                                               timestamp)
    annotations = create_annotation(image_assertions, additional_info, media,
                                    timestamp)
    event = map_to_annotation_event(annotations, str(uuid.uuid4()))
    logging.info('Created annotations: ' + json.dumps(event, indent=2))


def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {
        'annotations': annotations,
        'jobId': job_id
    }


def create_annotation(image_assertions: List[Dict[str, Any]],
                      additional_info: Dict[str, Any], digital_media: dict,
                      timestamp: str) -> List[Dict]:
    """
    Builds an annotation out of the assertions created by the Pillow imaging library
    :param additional_info: Other oa values to add
    :param image_assertions: assertions declared by library
    :param digital_media: json of the digital-media object
    :param timestamp: formatted date time
    :return: List of annotations
    """
    annotations = list()
    ods_agent = shared.get_agent()
    oa_selector = shared.build_class_selector("$ods:hasAssertion")

    for assertion in image_assertions:
        annotation = shared.map_to_annotation(ods_agent, timestamp, assertion,
                                              oa_selector,
                                              digital_media[shared.ODS_ID],
                                              digital_media[shared.ODS_TYPE],
                                              CODE_BASE)
        annotations.append(annotation)
    additional_info_annotation = shared.map_to_annotation(ods_agent, timestamp,
                                                          additional_info,
                                                          shared.build_field_selector(
                                                              DCTERMS_FORMAT),
                                                          digital_media[
                                                              shared.ODS_ID],
                                                          digital_media[
                                                              shared.ODS_TYPE],
                                                          CODE_BASE)
    if digital_media.get(DCTERMS_FORMAT) is not None:
        additional_info_annotation['oa:motivation'] = 'oa:editing'
    annotations.append(additional_info_annotation)
    return annotations


def publish_annotation_event(annotation_event: Dict,
                             producer: KafkaProducer) -> None:
    """
      Send the annotation to the Kafka topic
      :param annotation_event: The formatted annotation event
      :param producer: The initiated Kafka producer
      :return: Will not return anything
      """
    logging.info('Publishing annotation: ' + str(annotation_event))
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation_event)


def get_image_measurements(image_uri: str, timestamp: str) -> Tuple[
    List[Dict[str, Any]], Dict[str, Any]]:
    """
    Checks if the Image url works and gathers metadata information from the image
    :param image_uri: The image url from which we will gather metadata
    :param timestamp: time of the annotation
    :return: Returns a Dict of assertions about the image and any additional information
    """
    ods_agent = shared.get_agent()
    assertions = list()
    assertions.append(
        build_assertion(timestamp, ods_agent, 'ac:variant', 'acvariant:v008',
                        None))
    img_format = ''
    try:
        img = Image.open(requests.get(image_uri, stream=True).raw)
        img_file = BytesIO()
        img.save(img_file, img.format, quality='keep')
        assertions.append(
            build_assertion(timestamp, ods_agent, 'exif:PixelXDimension',
                            str(img.width),
                            'pixel'))
        assertions.append(
            build_assertion(timestamp, ods_agent, 'exif:PixelYDimension',
                            str(img.height),
                            'pixel'))
        assertions.append(
            build_assertion(timestamp, ods_agent, DCTERMS_FORMAT,
                            img.format.lower(),
                            None))
        assertions.append(
            build_assertion(timestamp, ods_agent, 'dcterms:extent',
                            str(round(img_file.tell() / 1000000, 2)),
                            "MB"))
        img_format = img.format.lower()
    except (FileNotFoundError, UnidentifiedImageError, MissingSchema):
        logging.exception('Failed to retrieve picture')
    if img_format:
        format_dict = {
            "format": img_format
        }
    else:
        format_dict = {}

    return assertions, format_dict


def build_assertion(timestamp: str, ods_agent: Dict, msmt_type: str,
                    msmt_value: str, unit) -> Dict:
    assertion = {
        shared.AT_TYPE: 'ods:Assertion',
        'dwc:measurementDeterminedDate': timestamp,
        'dwc:measurementType': msmt_type,
        'dwc:measurementValue': msmt_value,
        'ods:AssertionByAgent': ods_agent,
        'ods:assertionProtocol': 'Image processing with Python Pillow library',
        'ods:assertionProtocolID': CODE_BASE
    }
    if unit is not None:
        assertion['dwc:measurementUnit'] = unit
    return assertion


if __name__ == '__main__':
    start_kafka()
    # run_local('https://dev.dissco.tech/api/v1/digital-media/TEST/95K-GH1-562')
