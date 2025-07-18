import json
import logging
import os
import uuid

import requests
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
import numpy as np

from detectron2.config import get_cfg
from detectron2.engine.defaults import DefaultPredictor
from detectron2 import model_zoo
from typing import Tuple, Any, Dict, List
import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


## Be aware that this code is deprecated and will be removed in the future.
## It is kept for reference, the new code is in the `herbarium-sheet-plant-organ-segmentation` repository.


def start_kafka(predictor: DefaultPredictor) -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    :param predictor: The predictor which will be used to run the object detection
    """
    consumer = KafkaConsumer(
        os.environ.get("KAFKA_CONSUMER_TOPIC"),
        group_id=os.environ.get("KAFKA_CONSUMER_GROUP"),
        bootstrap_servers=[os.environ.get("KAFKA_CONSUMER_HOST")],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_PRODUCER_HOST")],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    for msg in consumer:
        try:
            logging.info(msg.value)
            json_value = msg.value
            shared.mark_job_as_running(json_value.get("jobId"))
            digital_media = json_value.get("object")
            additional_info_annotations, width, height = run_object_detection(
                digital_media.get("ac:accessURI"), predictor
            )
            annotations = map_result_to_annotation(digital_media, additional_info_annotations, width, height)
            event = map_to_annotation_event(annotations, json_value.get("jobId"))
            send_updated_opends(event, producer)
        except Exception as e:
            logging.exception(e)


def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {"annotations": annotations, "jobId": job_id}


def map_result_to_annotation(
    digital_media: Dict,
    additional_info_annotations: List[Dict[str, Any]],
    width: int,
    height: int,
) -> List[Dict[str, Any]]:
    """
    Builds the annotation records (one per ROI) from the prediction result.
    :param digital_media: The Digital Media Json
    :param additional_info_annotations: The additional info from the object detection
    :param width: The width of the image, used to calculate the ROI
    :param height: the height of the image, used to calculate the ROI
    :return: Returns a list of annotations (one per ROI), can be empty
    """
    annotations = []
    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()

    for value in additional_info_annotations:
        oa_value = {
            "class": value["class"],
            "score": value["score"],
        }
        oa_selector = shared.build_fragment_selector(value, width, height)
        annotation = shared.map_to_annotation(
            ods_agent,
            timestamp,
            oa_value,
            oa_selector,
            digital_media[shared.ODS_ID],
            digital_media[shared.ODS_TYPE],
            "https://github.com/2younis/plant-organ-detection",
        )
        annotations.append(annotation)
    return annotations


def send_updated_opends(event: dict, producer: KafkaProducer) -> None:
    """
    Send the event with jobId and annotations to the Kafka topic
    :param event: The event
    :param producer: The Kafka producer, topic will come from env variable
    :return: Nothing
    """
    logging.info("Publishing annotation: " + json.dumps(event))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), event)


def run_object_detection(image_uri: str, predictor: DefaultPredictor) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Checks if the Image url works and gathers metadata information from the image.
    :param image_uri: The image url from which we will gather metadata
    :param predictor: The model used in making the predictions
    :return: Returns a list of additional info about the image
    """

    try:
        img = Image.open(requests.get(image_uri, stream=True).raw)
        width, height = img.size
        predictions = predictor(np.array(img))
        instances = predictions["instances"]
        result = []

        class_names = ["leaf", "flower", "fruit", "seed", "stem", "root"]
        """
        Per template these are according to model training (pay attention to the order!):
        https://github.com/2younis/plant-organ-detection/blob/master/train_net.py
        """
        boxes = instances.pred_boxes.tensor.numpy()
        classes = instances.pred_classes
        scores = instances.scores.numpy()
        num_instances = len(boxes)
        logging.info("Detected %d instances" % num_instances)
        for i in range(num_instances):
            result.append(
                {
                    "class": class_names[classes[i]],
                    "score": float(scores[i]),
                    "boundingBox": [int(x) for x in boxes[i]],
                }
            )

        return result, width, height
    except FileNotFoundError:
        logging.exception("Failed to retrieve picture")
        return [], -1, -1


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the media data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Media to the API (for example
    https://dev.dissco.tech/api/v1/digital-media/TEST/GG9-1WB-N90
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    json_value = json.loads(response.content).get("data")
    digital_media = json_value.get("attributes")
    additional_info_annotations, width, height = run_object_detection(digital_media.get("ac:accessURI"), predictor)
    annotations = map_result_to_annotation(digital_media, additional_info_annotations, width, height)
    event = map_to_annotation_event(annotations, str(uuid.uuid4()))
    logging.info("Created annotations: " + json.dumps(event))


if __name__ == "__main__":
    cfg = get_cfg()
    cfg.merge_from_file(model_zoo.get_config_file("PascalVOC-Detection/faster_rcnn_R_50_FPN.yaml"))
    cfg.merge_from_file("config/custom_model_config.yaml")
    cfg.freeze()
    predictor = DefaultPredictor(cfg)

    start_kafka(predictor)
    # run_local("https://dev.dissco.tech/api/v1/digital-media/TEST/GG9-1WB-N90")
