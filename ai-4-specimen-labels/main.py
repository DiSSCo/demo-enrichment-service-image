import json
import logging
import os
from typing import Dict, Any, List

import requests
from kafka import KafkaConsumer, KafkaProducer
import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
OA_BODY = "oa:hasBody"
DWC_LOCALITY = "dwc:locality"
ODS_HAS_EVENTS = "ods:hasEvents"
ODS_HAS_LOCATION = "ods:hasLocation"
OA_VALUE = "oa:value"
LOCATION_PATH = f"$['{ODS_HAS_EVENTS}'][*]['{ODS_HAS_LOCATION}']"
USER_AGENT = "Distributed System of Scientific Collections"


"""
This is a template for a simple MAS that calls an API and sends a list of annotations back to DiSSCo. 
"""

def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
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
        # Indicates to DiSSCo the message has been received by the mas and the job is running.
        # DiSSCo then informs the user of this development
        shared.mark_job_as_running(job_id=json_value.get("jobId"))
        digital_object = json_value.get("object")
        try:
            annotations = build_annotations(digital_object)
            event = {"annotations": annotations, "jobId": json_value.get("jobId")}
            logging.info(f"Publishing annotation event: {json.dumps(event)}")
            publish_annotation_event(event, producer)
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")
            send_failed_message(json_value.get("jobId"), str(e), producer)


def build_query_string(digital_object: Dict[str, Any]) -> str:
    """
    Builds the query based on the digital object. Fill in your API call here
    :param digital_object: Target of the annotation
    :return: query string to some example API
    """
    # Use your API here
    return f"https://example.api.com/search?value={digital_object.get('some parameter of interest')}"


def publish_annotation_event(
        annotation_event: Dict[str, Any], producer: KafkaProducer
) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted list of annotations
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info(f"Publishing annotation: {str(annotation_event)}")
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)


def build_annotations(digital_object: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Given a target object, computes a result and maps the result to an openDS annotation
    :param digital_object: the target object of the annotation
    :return: List of annotations
    """
    # Your query here
    query_string = build_query_string(digital_object)
    timestamp = shared.timestamp_now()
    # Run API call and compute value(s) of the annotation
    response = run_api_call(query_string)

    if not response:
        # If the API call does not return a result, that information should still be captured in an annotation
        return [shared.map_to_empty_annotation(
            timestamp,
            "No results found",
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_TYPE],
            query_string,
        )]

    # todo for field in value make annotation
    annotations = list()
    return annotations


def run_api_call(query_string: str) -> List[str]:
    try :
        response = requests.get(query_string)
        response.raise_for_status()
        response_json = json.loads(response.content)
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException
    '''
    It is up to the MAS developer to determine the best format for the value of the annotation.
    '''
    oa_value = response_json['resultValue']
    # todo process value

    return oa_value



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


def run_local(specimen_id: str):
    """
    Runs script locally. Demonstrates using a specimen target
    :param specimen_id: A specimen ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_specimen = (
        requests.get(
            f"https://sandbox.dissco.tech/api/digital-specimen/v1/{specimen_id}"
        )
        .json()
        .get("data")
        .get("attributes")
    )

    specimen_annotations = build_annotations(digital_specimen)
    event = {specimen_annotations, "Some job ID"}
    logging.info(f"created annotation event: {json.dumps(event)}")


if __name__ == '__main__':
    # start_kafka()
    run_local("SANDBOX/4B5-3NT-PYS")
