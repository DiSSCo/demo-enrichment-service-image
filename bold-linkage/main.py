import json
import logging
import os
from uuid import uuid4
from typing import Dict, List

import requests
import pika
from requests.auth import HTTPBasicAuth
import shared
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel

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
        mas_job_record = map_to_annotation_event(specimen_data, result, json_value.get("jobId"))
        publish_annotation_event(mas_job_record, channel)
    except Exception as e:
        send_failed_message(json_value.get("jobId"), str(e), channel)


def map_to_annotation_event(specimen_data: Dict, results: List[Dict[str, str]], job_id: str) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the BOLD EU process identifier
    :param job_id: The job ID of the message
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if results is None:
        annotations = list()
    else:
        annotations = list(
            map(
                lambda result: map_result_to_annotation(specimen_data, result, timestamp),
                results,
            )
        )
    annotation_event = {"jobId": job_id, "annotations": annotations}
    return annotation_event


def map_result_to_annotation(specimen_data: Dict, result: Dict[str, str], timestamp: str) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The original specimen data
    :param result: The result from BOLD EU, contains the Bold EU processid and the queryString
    :param timestamp: A formatted timestamp of the current time
    :return: Returns a formatted annotation
    """
    ods_agent = shared.get_agent()
    oa_value = shared.map_to_entity_relationship(
        "hasBOLDEUProcessID",
        result["processid"],
        f"https://boldsystems.eu/record/{result['processid']}",
        timestamp,
        ods_agent,
    )
    oa_selector = shared.build_class_selector(shared.ER_PATH)
    return shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )


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


def run_api_call(specimen_data: Dict) -> List[Dict[str, str]]:
    """
    Calls BOLD EU API based on the available identifiers.
    It is possible that one Digital Specimen has multiple BOLD records.
    :param specimen_data: The JSON data of the Digital Specimen
    :return:  A list of results that contain the queryString and the BOLD EU process ids
    """
    # The endpoint for queries
    query_endpoint = "https://boldsystems.eu/api/query"

    # Get one or more specimen IDs. Considering that this will be a GET request this can't
    # be enormous - but perhaps 100 IDs is fine as a batch size.
    identifiers = list(
        map(
            lambda identifier: identifier.get("dcterms:identifier"),
            specimen_data.get("ods:hasIdentifiers"),
        )
    )
    # BOLD's API has a concept of 'scope' (here: 'ids') and 'subscope' (here: 'sampleid'),
    # where the value is the third part of a triple. All triples are joined with commas.
    query_value = ",".join([f"ids:sampleid:{id}" for id in identifiers])

    # Compose the URL and define the accept header
    headers = {"Accept": "application/json"}
    query_string = f"{query_endpoint}?query={query_value}&extent=full"

    # Do the request
    response = requests.get(
        query_string,
        headers=headers,
        auth=HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD")),
    )
    response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx

    # The response is a token from which a URL needs to be composed whose dereferencing
    # results in a set of records
    query_id = response.json()["query_id"]

    # Compose the URL for getting results. It is possible to page through results, so
    # you can specify how many you want per page (`length`) and the zero-based offset
    # assuming no more than 25 results per specimen are returned
    docs_endpoint = f"https://boldsystems.eu/api/documents/{query_id}?length=25&start=0"

    # Fetch the documents
    response = requests.get(
        docs_endpoint,
        headers=headers,
        auth=HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD")),
    )
    response.raise_for_status()  # Ensure the request was successful

    # Parse the response to get the records
    records = response.json()["data"]

    return list(
        map(
            lambda record: {
                "queryString": query_string,
                "processid": record["processid"],
            },
            records,
        )
    )


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/S0P-JMS-V4Q
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen = json.loads(response.content).get("data")
    specimen_data = specimen.get("attributes")
    result = run_api_call(specimen_data)
    mas_job_record = map_to_annotation_event(specimen_data, result, str(uuid4()))
    logging.info("Created annotations: " + json.dumps(mas_job_record, indent=2))


if __name__ == "__main__":
    run_rabbitmq()
    # run_local("https://dev.dissco.tech/api/digital-specimen/v1/TEST/MJG-GTC-5C2")
