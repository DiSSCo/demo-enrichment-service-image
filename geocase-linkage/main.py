import json
import logging
import os
import uuid
from typing import Dict, List

import requests
import pika
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


def map_to_annotation_event(specimen_data: Dict, results: List[Dict[str, str]], job_id: str) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if len(results) == 1 and results[0].get("errors") is not None:
        annotations = [
            shared.map_to_empty_annotation(
                timestamp,
                results[0].get("errors"),
                specimen_data,
                shared.ER_PATH,
                results[0].get("queryString"),
            )
        ]
    else:
        ods_agent = shared.get_agent()
        annotations = list(
            map(
                lambda result: map_result_to_annotation(specimen_data, result, timestamp, ods_agent),
                results,
            )
        )
    return {"jobId": job_id, "annotations": annotations}


def map_result_to_annotation(specimen_data: Dict, result: Dict, timestamp: str, ods_agent: Dict) -> Dict:
    """
    maps result of geocase linkage to an annotation
    :param specimen_data: specimen data
    :param result: result of the linkage search
    :param timestamp: formatted timestamp
    :param ods_agent: agent of this mas
    :return:
    """
    oa_value = shared.map_to_entity_relationship(
        "hasGeoCASeID",
        result["geocaseId"],
        f"https://geocase.eu/specimen/{result['geocaseId']}",
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
    :param job_id: The job ID of the MAS
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
    Calls GeoCASe API based on the available identifiers, unitId and/or recordURI.
    It is possible that one Digital Specimen has multiple GeoCASe records.
    If we get more than 5 GeoCASe hits we assume that something went wrong and we will not return any results.
    :param specimen_data: The JSON data of the Digital Specimen
    :return:  A list of results that contain the queryString and the geoCASe identifier
    """
    identifiers = get_identifiers_from_object(specimen_data)
    if identifiers and len(identifiers) > 0:
        query_string = build_query_string(identifiers)
        response = requests.get(query_string)
        response_json = json.loads(response.content)
        hits = response_json.get("response").get("numFound")
        if hits <= 5:
            return list(
                map(
                    lambda result: {
                        "queryString": query_string,
                        "geocaseId": result["geocase_id"],
                    },
                    response_json.get("response").get("docs"),
                )
            )
        else:
            logging.info(f"Too many hits ({hits}) were found for specimen: {specimen_data[shared.ODS_ID]}")
            return [
                {
                    "queryString": query_string,
                    "geocaseId": None,
                    "errors": "Failed to make the match, too many candidates",
                }
            ]
    else:
        logging.info(f"No relevant identifiers found for specimen: {specimen_data[shared.ODS_ID]}")
        return [
            {
                "queryString": "",
                "geocaseId": None,
                "errors": "Failed to make the match, No relevant identifiers found for specimen",
            }
        ]


def build_query_string(identifiers: Dict[str, str]):
    """
    Build query string from all identifiers in the Digital Specimen
    :param identifiers: All identifiers in the digital specimen
    :return: A formatted query string
    """
    query_string = "https://api.geocase.eu/v1/solr?q="
    for key, value in identifiers.items():
        if not query_string.endswith("q="):
            query_string = query_string + " AND "
        query_string = query_string + f'{key}:"{value}"'
    return query_string


def get_identifiers_from_object(specimen_data: Dict) -> Dict[str, str]:
    """
    Retrieve the correct identifiers from the Digital Specimen
    :param specimen_data: Json data of the Digital Specimen
    :return: The mapped relevant_identifiers (unitId and recordURI)
    """
    relevant_identifiers = {}
    for identifier in specimen_data["ods:hasIdentifiers"]:
        if identifier.get("dcterms:title") in ["abcd:unitID"]:
            relevant_identifiers["unitid"] = identifier.get("dcterms:identifier")
        if identifier.get("dcterms:title") in ["abcd:recordURI"]:
            relevant_identifiers["recordURI"] = identifier.get("dcterms:identifier")
    return relevant_identifiers


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/ZWL-YMS-4WY
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen_data = json.loads(response.content).get("data").get("attributes")
    result = run_api_call(specimen_data)
    mas_job_record = map_to_annotation_event(specimen_data, result, str(uuid.uuid4()))
    logging.info("Created annotations: " + str(mas_job_record))


if __name__ == "__main__":
    run_rabbitmq()
    # run_local('https://dev.dissco.tech/api/digital-specimen/v1/TEST/SGT-C68-7KY')
