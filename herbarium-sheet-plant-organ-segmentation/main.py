import json
import logging
import os
import uuid
import requests
from typing import Tuple, Any, Dict, List
from kafka import KafkaConsumer, KafkaProducer
from shared import shared

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service. 
    :param predictor: The predictor which will be used to run the plant organ segmentation

    """
    consumer = KafkaConsumer(
        os.environ.get('KAFKA_CONSUMER_TOPIC'),
        group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
        bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True
    )
    
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    for msg in consumer:
        try:
            logging.info(f"Received message: {str(msg.value)}")
            json_value = msg.value
            shared.mark_job_as_running(job_id=json_value.get('jobId'))  
            digital_object = json_value.get('object')
            additional_info_annotations, image_height, image_width = run_plant_organ_segmentation(digital_object.get('ac:accessURI')) 
            annotations = map_result_to_annotation(digital_object, additional_info_annotations, image_height, image_width)
            annotation_event = map_to_annotation_event(annotations, json_value['jobId'])

            logging.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
            publish_annotation_event(annotation_event, producer)
 
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")

def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {
        "annotations": annotations,
        "jobId": job_id
    }

def publish_annotation_event(annotation_event: Dict[str, Any], producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic.
    :param annotation_event: The formatted list of annotations
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info(f"Publishing annotation: {str(annotation_event)}")
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation_event)


def map_result_to_annotation(digital_object: Dict,
                             additional_info_annotations: List[Dict[str, Any]], image_height: int,
                             image_width: int):
    """
    Given a target object, computes a result and maps the result to an openDS annotation.
    :param digital_object: the target object of the annotation
    :return: List of annotations
    """
    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()
    annotations = list()

    for annotation in additional_info_annotations:
        oa_value = {
            "boundingBox": annotation.get('boundingBox'), 
            "class": annotation.get('class'),
            "score": annotation.get('score'),
            "areaInPixel": annotation.get('areaInPixel'),
            "one_cm_in_pixel": annotation.get('one_cm_in_pixel'),
            "areaInCm2": annotation.get('areaInCm2'),
            "polygon": annotation.get('polygon')
            }
        oa_selector = shared.build_fragment_selector(annotation, image_width, image_height)
        annotation = shared.map_to_annotation(ods_agent, timestamp, oa_value, oa_selector, digital_object[shared.ODS_ID],
                                              digital_object[shared.ODS_TYPE],
                                              'https://github.com/RajapreethiRajendran/demo-enrichment-service-image')
        annotations.append(annotation)
    
    return annotations


def run_plant_organ_segmentation(image_uri: str) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    post the image url request to plant organ segmentation service.
    :param image_uri: The image url from which we will gather metadata
    :return: Returns a list of additional info about the image
    """
    payload = {
        "image_url": image_uri
    }
    annotations_list = []
    auth_info = {
        'username': os.environ.get('PLANT_ORGAN_SEGMENTATION_USER'),
        'password': os.environ.get('PLANT_ORGAN_SEGMENTATION_PASSWORD')
    }
    try:
        response = requests.post("https://webapp.senckenberg.de/dissco-mas-prototype/plant_organ_segmentation", auth=(auth_info['username'], auth_info['password']), json=payload)
        response.raise_for_status()
        response_json = response.json()
        if len(response_json) == 0:
            logging.info('No results for this herbarium sheet: ' + payload["image_url"])      
            return [], -1, -1  
        else:
            for response in response_json.get('output', []):                
                annotations_list.append({
                    "boundingBox": response.get('boundingBox'), 
                    "class": response.get('class'),
                    "score": response.get('score'),
                    "areaInPixel": response.get('areaInPixel'),
                    "one_cm_in_pixel": response.get('one_cm_in_pixel'),
                    "areaInCm2": response.get('areaInCm2'),
                    "polygon": response.get('polygon')
                    })  
            image_height = response_json.get('image_height')
            image_width = response_json.get('image_width')
            return annotations_list, image_height, image_width
            
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/digital-media/TEST/GG9-1WB-N90
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    json_value = json.loads(response.content).get('data')
    digital_object = json_value.get('attributes')
    additional_info_annotations, image_height, image_width = run_plant_organ_segmentation(digital_object.get('ac:accessURI')) 
    annotations = map_result_to_annotation(digital_object, additional_info_annotations, image_height, image_width)

    event = map_to_annotation_event(annotations, str(uuid.uuid4()))
    logging.info('Created annotations: ' + json.dumps(event))



if __name__ == '__main__':
    start_kafka()
    #run_local("https://dev.dissco.tech/api/v1/digital-media/TEST/GG9-1WB-N90")
