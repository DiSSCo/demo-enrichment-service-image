import json
import logging
import os
import uuid

import pika
import requests as requests
from typing import Dict
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel

import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


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
        shared.mark_job_as_running(json_value.get("jobId"))
        specimen_data = json_value.get("object")
        result = run_api_call(specimen_data)
        annotation_event = map_to_annotation_event(specimen_data, result, json_value.get("jobId"))
        publish_annotation_event(annotation_event, channel)
    except Exception as e:
        send_failed_message(json_value.get("jobId"), str(e), channel)


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


def map_to_annotation_event(specimen_data: Dict, result: Dict[str, str], job_id: str) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result which contains either the GBIF ID or an error message
    :param job_id: The job ID of the message
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if result.get("error_message") is not None:
        return {
            "jobId": job_id,
            "annotations": [
                shared.map_to_empty_annotation(
                    timestamp,
                    result.get("error_message"),
                    specimen_data,
                    shared.ER_PATH,
                    result.get("queryString"),
                )
            ],
        }
    ods_agent = shared.get_agent()
    oa_value = shared.map_to_entity_relationship(
        "hasGbifID",
        result.get("gbifID"),
        f"https://www.gbif.org/occurrence/{result.get('gbifID')}",
        timestamp,
        ods_agent,
    )
    oa_selector = shared.build_class_selector(shared.ER_PATH)
    annotation = shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )

    return {"jobId": job_id, "annotations": [annotation]}


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


def run_api_call(specimen_data: Dict) -> Dict[str, str]:
    """
    Calls GBIF API based on the occurrenceID, catalogNumber and basisOfRecord
    :param specimen_data: The JSON data of the Digital Specimen
    :return: The result from the API, which contains either the GBIF ID or an error message
    """
    identifiers = get_identifiers_from_object(specimen_data)
    query_string = (
        f"https://api.gbif.org/v1/occurrence/search?occurrenceID="
        f"{identifiers.get('occurrenceID')}"
        f"&basisOfRecord={specimen_data.get('dwc:basisOfRecord')}"
    )
    if specimen_data.get("catalogNumber") is not None:
        query_string = query_string + f"&catalogNumber={identifiers.get('catalogNumber')}"
    response = requests.get(query_string)
    response_json = json.loads(response.content)
    if response_json.get("count") == 1:
        logging.info("Successfully retrieved a single result from GBIF based on the identifiers")
        return {
            "queryString": query_string,
            "gbifID": response_json.get("results")[0].get("gbifID"),
        }
    elif response_json["count"] == 0:
        logging.info("No results were returned, unable to create a relationship")
        return {
            "queryString": query_string,
            "error_message": "Failed to make the match, no match could be created",
        }
    else:
        logging.info("More than one result returned, unable to create a relationship")
        return {
            "queryString": query_string,
            "error_message": "Failed to make the match, too many candidates",
        }


def get_identifiers_from_object(specimen_data: Dict) -> Dict[str, str]:
    """
    Retrieve the correct identifiers from the Digital Specimen
    :param specimen_data: Json data of the Digital Specimen
    :return: The mapped relevant_identifiers (occurrenceID and catalogNumber)
    """
    relevant_identifiers = {}
    for identifier in specimen_data.get("ods:hasIdentifiers"):
        if identifier.get("dcterms:title") in ["dwc:occurrenceID", "abcd:unitGUID"]:
            relevant_identifiers["occurrenceID"] = identifier.get("dcterms:identifier")
        if identifier.get("dcterms:title") in ["dwc:catalogNumber", "abcd:unitID"]:
            relevant_identifiers["catalogNumber"] = identifier.get("dcterms:identifier")
    return relevant_identifiers


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/digital-specimen/TEST/TYB-XNH-53H
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen_data = json.loads(response.content).get("data").get("attributes")
    result = run_api_call(specimen_data)
    annotations = map_to_annotation_event(specimen_data, result, str(uuid.uuid4()))
    logging.info("Created annotations: " + json.dumps(annotations, indent=2))


if __name__ == "__main__":
    run_rabbitmq()
    # run_local("https://sandbox.dissco.tech/api/digital-specimen/v1/SANDBOX/A7D-9PL-3YP")
