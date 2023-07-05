import json
import logging
import os

import requests as requests
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
import numpy as np
from datetime import datetime, timezone

from detectron2.config import get_cfg
from detectron2.engine.defaults import DefaultPredictor
from detectron2 import model_zoo
from detectron2.data.detection_utils import convert_PIL_to_numpy

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def start_kafka(predictor: DefaultPredictor) -> None:
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
        logging.info(msg.value)
        json_value = msg.value
        image_uri = json_value['data']['ac:accessURI']
        additional_info_annotations = run_object_detection(image_uri, predictor)
        logging.info('Publishing the result: %s', json_value)
        annotation = map_to_annotation(json_value, additional_info_annotations)
        send_updated_opends(annotation, producer)


def map_to_annotation(json_value, additional_info_annotations) -> dict:
    values = []
    for value in additional_info_annotations:
        values.append(
            {
                'class': value['class'],
                'score': value['score'],
                'selector': {
                    'type': 'FragmentSelector',
                    'conformsTo': 'http://www.w3.org/TR/media-frags/',
                    'value': "xywh=" + str(value['boundingBox'][0]) + ',' + str(value['boundingBox'][1]) + ',' + str(
                        value['boundingBox'][2]) + ',' + str(value['boundingBox'][3])
                }
            }
        )
    annotation = {
        'type': 'Annotation',
        'motivation': 'https://hdl.handle.net/pid-machine-enrichment',
        'creator': 'https://hdl.handle.net/enrichment-service-pid',
        'created': timestamp_now(),
        'target': {
            'id': json_value['id'],
            'type': 'https://hdl.handle.net/21...'
        },
        'body': {
            'source': json_value['data']['ac:accessURI'],
            'purpose': 'describing',
            'values': values
        }
    }
    logging.info(annotation)
    return annotation


def timestamp_now():
    timestamp = str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone

def send_updated_opends(annotation: dict, producer: KafkaProducer) -> None:
    producer.send('annotation', annotation)


def run_object_detection(image_uri: str, predictor: DefaultPredictor) -> list:
    """
    Checks if the Image url works and gathers metadata information from the image
    :param image_uri: The image url from which we will gather metadata
    :return: Returns a list of additional info about the image
    """
    try:
        img = Image.open(requests.get(image_uri, stream=True).raw)
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

        return annotations_result
    except FileNotFoundError:
        logging.exception('Failed to retrieve picture')
        return []


if __name__ == '__main__':
    cfg = get_cfg()
    cfg.merge_from_file(model_zoo.get_config_file('PascalVOC-Detection/faster_rcnn_R_50_FPN.yaml'))
    cfg.merge_from_file('config/custom_model_config.yaml')
    cfg.freeze()
    predictor = DefaultPredictor(cfg)

    start_kafka(predictor)
    # run_object_detection('https://www.unimus.no/felles/bilder/web_hent_bilde.php?id=14894911&type=jpeg', predictor)
