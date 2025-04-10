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
    oa_values = run_api_call(timestamp, query_string)

    if not oa_values:
        # If the API call does not return a result, that information should still be captured in an annotation
        return [shared.map_to_empty_annotation(
            timestamp,
            "No results found",
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_TYPE],
            query_string,
        )]

    annotations = list()
    """
    It is up to the developer to determine the most appropriate selector. A Class 
    Selector is used if the annotation targets a whole class (or entire specimen), 
    a Term Selector is used if the annotation targets a specific term, and a Fragment
    Selector is used if the annotation targets a Region of Interest (media only). 
    Templates for all 3 selectors are provided in the data_model.py library

    For Editing, Commenting, and Deleting motivations, the JSON Path of the selector must exist in the target
    (e.g. You can not target the 10th Assertion if there are only 5 assertions on the target)
    For Adding motivations, you may target a path that doesn't exist, as you are adding information to the specimen
    (e.g. adding the first Assertion to a target) 
    """

    selector_assertion = shared.build_class_selector("$['ods:hasAssertions'][0]")
    selector_entity_relationship = shared.build_class_selector(
        "$['ods:hasEntityRelationships'][0]"
    )
    selector_term = shared.build_term_selector("$['ods:topicDomain']")

    # Make an annotation for each oa:value produced
    annotations.append(
        shared.map_to_annotation_str_val(
            shared.get_agent(),
            timestamp,
            oa_values[0],
            selector_assertion,
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_ID],
            query_string,
            "ods:adding"
        )
    )
    annotations.append(
        shared.map_to_annotation_str_val(
            shared.get_agent(),
            timestamp,
            oa_values[1],
            selector_entity_relationship,
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_ID],
            query_string,
            "ods:adding"
        )
    )
    annotations.append(
        shared.map_to_annotation_str_val(
            shared.get_agent(),
            timestamp,
            oa_values[3],
            selector_term,
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_ID],
            query_string,
            "ods:commenting"
        )
    )
    return annotations


def run_api_call(timestamp: str, query_string: str) -> List[str]:
    """
    Run API call or performs some computation on the target object
    :param timestamp: The timestamp of the annotation
    :query_string: Query string for the API call
    :return: Value of the annotation (maps to oa:value)
    :raises: request exception on failed API call
    """
    try:
        response = requests.get(query_string)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException

    response_json = json.loads(response.content)
    """
    It is up to the MAS developer to determine the best format for the value of the annotation.
    It may be appropriate to send an annotation as an Entity Relationship, if it describes
    a relationship to an external resource, or an Assertion, if the mas has performed 
    some computation or measurement. It may also map to the Georeference class. 
    """
    # Return a response like this if the result is a computation or measurement
    assertion = json.dumps(
        {
            shared.AT_TYPE: "ods:Assertion",
            "dwc:measurementDeterminedDate": timestamp,
            "dwc:measurementType": "measurement",
            "dwc:measurementValue": "123",
            "ods:hasAgents": shared.get_agent(),
            "dwc:measurementMethod": "Data processed using some library",
        }
    )
    # Return this if response is an entity relationship
    entity_relationship = json.dumps(
        shared.map_to_entity_relationship(
            "hasRelatedResourceIdentifier",
            response_json["resourceID"],
            "https://example.com/relatedResourceId",
            timestamp, shared.get_agent()
        )
    )
    # Return something else - such as the raw response - if the response can not be structured in another wa

    """
    It is possible your MAS produces multiple annotations - this is supported. 
    One annotation can be created for each oa:value computed in this step
    """
    return [assertion, entity_relationship, json.dumps(response_json)]


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
