import json
import logging
import os

import requests
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
import numpy as np
from datetime import datetime, timezone

from detectron2.config import get_cfg
from detectron2.engine.defaults import DefaultPredictor
from detectron2 import model_zoo
from typing import Tuple

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
os.environ["KAFKA_CONSUMER_TOPIC"] = 'digital-specimen'
os.environ["KAFKA_CONSUMER_GROUP"] = 'group-1'
os.environ["KAFKA_CONSUMER_HOST"] = 'localhost:9092'
os.environ["KAFKA_PRODUCER_HOST"] = 'localhost:9092'


def start_kafka(predictor: DefaultPredictor) -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    :param name: The topic name the Kafka listener needs to listen to
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'),
                             group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[
                                 os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(
                                 m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
        value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        logging.info(msg.value)
        json_value = msg.value
        image_uri = json_value['object']['digitalEntity']['ac:accessURI']
        additional_info_annotations, width, height = run_object_detection(image_uri, predictor)
        logging.info('Publishing the result: %s', json_value)
        event = map_to_annotation(json_value, additional_info_annotations, width, height)
        event["jobId"] = json_value['jobId']
        send_updated_opends(event, producer)


def map_to_annotation(json_value, additional_info_annotations, width, height) -> dict:
    annotations = []
    for value in additional_info_annotations:
        body_values = [{
            'class': value['class'],
            'score': value['score'],
        }]
        selector = {
            'oa:selector': {
                'ods:type': 'FragmentSelector',
                'dcterms:conformsTo': 'http://www.w3.org/TR/media-frags/',
                'ac:hasRoi': {
                    "ac:xFrac": value['boundingBox'][0] / width,
                    "ac:yFrac": value['boundingBox'][1] / height,
                    "ac:widthFrac": (value['boundingBox'][2] - value['boundingBox'][0])/width,
                    "ac:heightFrac": (value['boundingBox'][3] - value['boundingBox'][1]) / height
                }
            }
        }
        annotation = {
            'rdf:type': 'Annotation',
            'oa:motivation': 'ods:adding',
            'oa:target': {
                'ods:id': json_value['object']['digitalEntity']['ods:id'],
                'ods:type': 'http://hdl.handle.net/21.T11148/bbad8c4e101e8af01115', # handle for digitalMedia Type
                'oa:selector': selector
            },
            'oa:body': {
                'ods:type': 'TextualBody',
                'dcterms:reference': 'https://github.com/2younis/plant-organ-detection/blob/master/train_net.py',
                'oa:value': body_values
            }
        }
        annotations.append(annotation)
    logging.info(annotations)
    event = {
        'annotations': annotations
    }
    return event


def timestamp_now():
    timestamp = str(
        datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone


def send_updated_opends(annotation: dict, producer: KafkaProducer) -> None:
    producer.send('annotation', annotation)


def run_object_detection(image_uri: str, predictor: DefaultPredictor) -> Tuple[list, int, int]:
    """
    Checks if the Image url works and gathers metadata information from the image
    :param image_uri: The image url from which we will gather metadata
    :return: Returns a list of additional info about the image
    """
    try:
        img = Image.open(requests.get(image_uri, stream=True).raw)
        width, height = img.size
        predictions = predictor(np.array(img))
        instances = predictions['instances']
        annotations_result = []

        class_names = ['leaf', 'flower', 'fruit', 'seed', 'stem', 'root']
        """
        Per template these are according to model training (pay attention to the order!):
        https://github.com/2younis/plant-organ-detection/blob/master/train_net.py
        """
        boxes = instances.pred_boxes.tensor.numpy()
        classes = instances.pred_classes
        scores = instances.scores.numpy()
        num_instances = len(boxes)
        logging.info('Detected %d instances' % num_instances)
        for i in range(num_instances):
            annotations_result.append({
                'class': class_names[classes[i]],
                'score': float(scores[i]),
                'boundingBox': [int(x) for x in boxes[i]]
            })

        return annotations_result, width, height
    except FileNotFoundError:
        logging.exception('Failed to retrieve picture')
        return [], -1, -1


if __name__ == '__main__':
    cfg = get_cfg()
    cfg.merge_from_file(model_zoo.get_config_file('PascalVOC-Detection/faster_rcnn_R_50_FPN.yaml'))
    cfg.merge_from_file('config/custom_model_config.yaml')
    cfg.freeze()
    predictor = DefaultPredictor(cfg)

    start_kafka(predictor)
    # annotation, width, height = run_object_detection('https://www.unimus.no/felles/bilder/web_hent_bilde.php?id=14894911&type=jpeg', predictor)
    # map_to_annotation(None, annotation, width, height), indent=2))
