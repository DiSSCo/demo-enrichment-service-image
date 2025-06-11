import json
import logging
import os
from typing import Dict, Any, List

import requests
import pika
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel
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


def run_rabbitmq() -> None:
    """
    Start a RabbitMQ consumer and process the messages by unpacking the image.
    When done, it will publish an annotation to annotation processing service
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            os.environ.get("RABBITMQ_HOST"),
            credentials=pika.PlainCredentials(os.environ.get("RABBITMQ_USER"), os.environ.get("RABBITMQ_PASSWORD")),
        )
    )
    channel = connection.channel()
    channel.basic_consume(queue=os.environ.get("RABBITMQ_QUEUE"), on_message_callback=process_message, auto_ack=True)
    channel.start_consuming()


def process_message(channel: BlockingChannel, method: Method, properties: Properties, body: bytes) -> None:
    """
    Callback function to process the message from RabbitMQ. This method will be called for each message received.
    We publish this annotation through the channel on a RabbitMQ exchange.
    :param channel: The RabbitMQ channel, which we will use to publish the resulting annotation
    :param method: The method used to send the message, not currently used
    :param properties: Properties of the message, not currently used
    :param body: The message body in bytes
    :return:
    """
    json_value = json.loads(body.decode("utf-8"))
    try:
        shared.mark_job_as_running(job_id=json_value.get("jobId"))
        digital_object = json_value.get("object")
        annotations = build_annotations(digital_object)
        event = {"annotations": annotations, "jobId": json_value.get("jobId")}
        logging.info(f"Publishing annotation event: {json.dumps(event)}")
        publish_annotation_event(event, channel)
    except Exception as e:
        send_failed_message(json_value.get("jobId"), str(e), channel)


def build_query_string(digital_object: Dict[str, Any]) -> str:
    """
    Builds the query based on the digital object. Fill in your API call here
    :param digital_object: Target of the annotation
    :return: query string to some example API
    """
    # Use your API here
    return f"https://example.api.com/search?value={digital_object.get('some parameter of interest')}"


def publish_annotation_event(annotation_event: Dict, channel: BlockingChannel) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted annotation event
    :param channel: A RabbitMQ BlockingChannel to which we will publish the annotation
    :return: Will not return anything
    """
    logging.info("Publishing annotation: " + str(annotation_event))
    channel.basic_publish(
        exchange=os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-exchange"),
        routing_key=os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation"),
        body=json.dumps(annotation_event).encode("utf-8"),
    )


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
        return [
            shared.map_to_empty_annotation(
                timestamp,
                "No results found",
                digital_object[shared.ODS_ID],
                digital_object[shared.ODS_TYPE],
                query_string,
            )
        ]

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
    selector_entity_relationship = shared.build_class_selector("$['ods:hasEntityRelationships'][0]")
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
            "ods:adding",
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
            "ods:adding",
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
            "ods:commenting",
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
            timestamp,
            shared.get_agent(),
        )
    )
    # Return something else - such as the raw response - if the response can not be structured in another wa

    """
    It is possible your MAS produces multiple annotations - this is supported. 
    One annotation can be created for each oa:value computed in this step
    """
    return [assertion, entity_relationship, json.dumps(response_json)]


def send_failed_message(job_id: str, message: str, channel: BlockingChannel) -> None:
    """
    Send a message to the RabbitMQ queue indicating that the job has failed
    :param job_id: The job ID of the message
    :param message: The error message to be sent
    :param channel: A RabbitMQ BlockingChannel to which we will publish the error message
    :return: Will not return anything
    """
    logging.error(f"Job {job_id} failed with error: {message}")
    mas_failed = {"jobId": job_id, "errorMessage": message}
    channel.basic_publish(
        exchange=os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-failed-exchange"),
        routing_key=os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation-failed"),
        body=json.dumps(mas_failed).encode("utf-8"),
    )


def run_local(specimen_id: str):
    """
    Runs script locally. Demonstrates using a specimen target
    :param specimen_id: A specimen ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_specimen = (
        requests.get(f"https://sandbox.dissco.tech/api/digital-specimen/v1/{specimen_id}")
        .json()
        .get("data")
        .get("attributes")
    )

    specimen_annotations = build_annotations(digital_specimen)
    event = {specimen_annotations, "Some job ID"}
    logging.info(f"created annotation event: {json.dumps(event)}")


if __name__ == "__main__":
    # run_rabbitmq()
    run_local("SANDBOX/4B5-3NT-PYS")
