import json
import logging
import os
import uuid
import requests
from typing import Any, Dict, List

from shared import shared
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
ods_has_events = "ods:hasEvents"
dwc_locality = "dwc:locality"

def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service.
    :param predictor: The predictor which will be used to extract the ontologies for dwc:habitat and dwc:locality digital specimen  

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
        logging.info(f"Received message: {str(msg.value)}")
        json_value = msg.value
        try:
            shared.mark_job_as_running(job_id=json_value.get("jobId"))
            digital_object = json_value.get("object")
            habitat = digital_object.get(ods_has_events)[0].get("dwc:habitat")
            locality = digital_object.get(ods_has_events)[0].get("ods:hasLocation").get(dwc_locality)
            if habitat or locality:
                additional_info_annotations = (
                    run_ontology_extraction(habitat, locality)
                )
                if len(additional_info_annotations[0]) == 0:
                    annotations = map_empty_result_annotation(digital_object, "No habitat ontologies are extracted by ontoGPT")
                    annotation_event = map_to_annotation_event(annotations, json_value["jobId"])
                    logging.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
                    publish_annotation_event(annotation_event, producer)
                else:
                    annotations = map_result_to_annotation(
                        digital_object, additional_info_annotations
                    )
                    annotation_event = map_to_annotation_event(annotations, json_value["jobId"])

                    logging.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
                    publish_annotation_event(annotation_event, producer)
            else:
                annotations = map_empty_result_annotation(
                    digital_object, "No habitat or locality information is found in digital specimen"
                )
                annotation_event = map_to_annotation_event(annotations, json_value["jobId"])
                logging.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
                publish_annotation_event(annotation_event, producer)

                
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")
            send_failed_message(json_value["jobId"], str(e), producer)


def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {"annotations": annotations, "jobId": job_id}


def publish_annotation_event(
        annotation_event: Dict[str, Any], producer: KafkaProducer
) -> None:
    """
    Send the annotation to the Kafka topic.
    :param annotation_event: The formatted list of annotations
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info(f"Publishing annotation: {str(annotation_event)}")
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)

def map_empty_result_annotation(digital_object: Dict, message: str):
    annotation = shared.map_to_empty_annotation(
                    timestamp = shared.timestamp_now(),
                    message= message,
                    target_data = digital_object,
                    selector = shared.build_term_selector(dwc_locality),
                    dcterms_ref = "https://github.com/RajapreethiRajendran/demo-enrichment-service-image"
                )
    return annotation


def map_result_to_annotation(
        digital_object: Dict,
        additional_info_annotations: List[Dict[str, Any]]):
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
            "id": annotation.get("id")  ,
            "label" : annotation.get("label")     
        }
        
        oa_selector = shared.build_term_selector(dwc_locality)
        annotation = shared.map_to_annotation_str_val(
            ods_agent,
            timestamp,
            oa_value,
            oa_selector,
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_TYPE],
            "https://github.com/RajapreethiRajendran/demo-enrichment-service-image",
            motivation = "oa:commenting"

        )
        annotations.append(annotation)

    return annotations

def run_ontology_extraction(habitat_text: str,location_text: str) -> List[Dict[str, Any]]:
    """
    post the image url request to plant organ segmentation service.
    :param image_uri: The image url from which we will gather metadata
    :return: Returns a list of additional info about the image
    """
    payload = {"input_text": habitat_text + " " + location_text}
    auth_info = {
        "username": os.environ.get("HABITAT_ONTOGPT_USER"),
        "password": os.environ.get("HABITAT_ONTOGPT_PASSWORD"),
    }
    response = requests.post(
        "https://webapp.senckenberg.de/dissco-ontogpt-mas-prototype/extract_ontogpt",
        auth=(auth_info["username"], auth_info["password"]),
        json=payload,
        timeout=600
    )
    response.raise_for_status()
    response_json = response.json()
    if len(response_json) == 0:
        logging.info("No results for this habitat: " + payload["input_text"])
        return [], -1, -1
    else:
        return response_json.get("named_entities")
    

def send_failed_message(job_id: str, message: str, producer: KafkaProducer) -> None:
    """
    Sends a failure message to the mas failure topic, mas-failed
    :param job_id: The id of the job
    :param message: The exception message
    :param producer: The Kafka producer
    """

    mas_failed = {
        "jobId": job_id,
        "errorMessage": message
    }
    producer.send("mas-failed", mas_failed)


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
    json_value = json.loads(response.content).get("data")
    digital_object = json_value.get("attributes")
    habitat = digital_object.get(ods_has_events)[0].get("dwc:habitat")
    locality = digital_object.get(ods_has_events)[0].get("ods:hasLocation").get(dwc_locality)      
    if habitat or locality:
        additional_info_annotations = (
            run_ontology_extraction(habitat, locality)
                )
        if len(additional_info_annotations[0]) == 0:
            annotations = map_empty_result_annotation(
                    digital_object, "No habitat ontologies are extracted by ontoGPT"
                )
            event = map_to_annotation_event(annotations, str(uuid.uuid4()))
            logging.info("Created annotations: " + json.dumps(event))   

        else:
            annotations = map_result_to_annotation(
                    digital_object, additional_info_annotations
                )
            event = map_to_annotation_event(annotations, str(uuid.uuid4()))
            logging.info("Created annotations: " + json.dumps(event))     
    else:
        annotations = map_empty_result_annotation(
                    digital_object, "No habitat or locality information is found in digital specimen"
                )
        event = map_to_annotation_event(annotations, str(uuid.uuid4()))
        
        logging.info("Created annotations: " + json.dumps(event))   
   


if __name__ == "__main__":
    start_kafka()
    #run_local("https://dev.dissco.tech/api/digital-specimen/v1/TEST/VHY-DC5-87F")
