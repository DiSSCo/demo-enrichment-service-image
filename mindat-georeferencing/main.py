import json
import logging
import os
import uuid
from typing import Dict, List, Any, Tuple

import requests
import pika
import shared
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

HAS_LOCATION = "ods:hasLocation"


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
        batching_requested = json_value["batchingRequested"]
        result, batch_metadata = run_georeference(specimen_data, batching_requested)
        mas_job_record = map_to_annotation_event(specimen_data, result, json_value.get("jobId"), batch_metadata)
        publish_annotation_event(mas_job_record, channel)
    except Exception as e:
        send_failed_message(json_value.get("jobId"), str(e), channel)


def map_to_annotation_event(
    specimen_data: Dict,
    results: List[Dict[str, str]],
    job_id: str,
    batch_metadata: List[Dict[str, Any]],
) -> Dict:
    """
    Map the result of the API call to an annotation
    :param batch_metadata: Information about the computation, if requested
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    batching_requested = len(batch_metadata) > 0
    ods_agent = shared.get_agent()
    if results is None:
        annotations = list(
            shared.map_to_empty_annotation(
                timestamp,
                "Unable to determine locality from specimen information",
                specimen_data,
                "$['ods:hasEvents']",
            )
        )
    else:
        annotations = list(
            map(
                lambda result: map_to_georeference_annotation(
                    specimen_data, result, timestamp, batching_requested, ods_agent
                ),
                results,
            )
        )
        annotations.extend(
            list(
                map(
                    lambda result: map_to_entity_relationship_annotation(
                        specimen_data, result, timestamp, batching_requested, ods_agent
                    ),
                    results,
                )
            )
        )
    annotation_event = {"jobId": job_id, "annotations": annotations}
    if batch_metadata:
        annotation_event["batchMetadata"] = batch_metadata
    return annotation_event


def build_batch_metadata(locality: str, place_in_batch: int) -> Dict[str, Any]:
    batch_metadata = {
        "ods:placeInBatch": place_in_batch,
        "searchParams": [
            {
                "inputField": "$['ods:hasEvents'][*][HAS_LOCATION]['dwc:locality']",
                "inputValue": locality,
            }
        ],
    }
    return batch_metadata


def map_to_entity_relationship_annotation(
    specimen_data: Dict,
    result: Dict[str, Any],
    timestamp: str,
    batching_requested: bool,
    ods_agent: Dict,
) -> Dict:
    """
    Map the result of the Mindat Locality API call to an entityRelationship annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Mindat Locality API call
    :param timestamp: The current timestamp
    :param batching_requested: Indicates if the scheduling party requested batching
    :return:  A single annotation with the relationship to the Mindat locality
    """
    oa_value = shared.map_to_entity_relationship(
        "hasMindatLocation",
        f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
        f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
        timestamp,
        ods_agent,
    )
    return wrap_oa_value(
        oa_value,
        result,
        specimen_data,
        timestamp,
        shared.ER_PATH,
        batching_requested,
        ods_agent,
    )


def map_to_georeference_annotation(
    specimen_data: Dict,
    result: Dict[str, Any],
    timestamp: str,
    batching_requested: bool,
    ods_agent: Dict,
) -> Dict:
    """
    Map the result of the Mindat Locality API call to a georeference annotation
    :param ods_agent: The agent creating the annotation
    :param batching_requested: Indicates if the scheduling party requested batching
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Mindat Locality API call
    :param timestamp: The current timestamp
    :return: A single annotation with the georeference information from the Mindat locality
    """
    oa_value = {
        "dwc:decimalLatitude": round(result["geo_reference_result"]["latitude"], 7),
        "dwc:decimalLongitude": round(result["geo_reference_result"]["longitude"], 7),
        "dwc:geodeticDatum": "WGS84",
        "dwc:hasAgents": [ods_agent],
        "dwc:georeferencedDate": timestamp,
        "dwc:georeferenceSources": f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
        "dwc:georeferenceProtocol": "Georeferenced against the Mindat Locality API based on the specimen "
        "locality string (dwc:locality)",
    }

    return wrap_oa_value(
        oa_value,
        result,
        specimen_data,
        timestamp,
        f"$['ods:hasEvents']['{result['result_index']}'][HAS_LOCATION]['ods:hasGeoReference']",
        batching_requested,
        ods_agent,
    )


def wrap_oa_value(
    oa_value: Dict,
    result: Dict[str, Any],
    specimen_data: Dict,
    timestamp: str,
    oa_class: str,
    batching_requested: bool,
    ods_agent: Dict,
) -> Dict:
    """
    Generic method to wrap the oa_value into an annotation object
    :param ods_agent: the agent creating the annotation
    :param batching_requested: Indicates if the scheduling party requested batching
    :param oa_value: The value that contains the result of the MAS
    :param result: The result of the Mindat Locality API call
    :param specimen_data: The JSON value of the Digital Specimen
    :param timestamp: The current timestamp
    :param oa_class: The name of the class to which the class annotation points
    :return: Returns an annotation with all the relevant metadata
    """
    oa_selector = shared.build_class_selector(oa_class)
    annotation = shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )
    # If batching is requested, the annotation must contain a "placeInBatch" value equal to the corresponding batch metadata
    if batching_requested:
        annotation["ods:placeInBatch"] = result["result_index"]

    return annotation


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


def run_georeference(
    specimen_data: Dict, batching_requested: bool
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Run the value in the dwc:locality field through the Mindat Locality API and return the highest result per occurrence
    :param batching_requested: Indicates if the scheduling party requested batching
    :param specimen_data: The full specimen object
    :return: List of the results including some metadata
    """
    events = specimen_data.get("ods:hasEvents")
    result_list = list()
    batch_metadata = list()
    for index, event in enumerate(events):
        if (event.get(HAS_LOCATION) is not None and event.get(HAS_LOCATION).get("dwc:locality")) is not None:
            location = event.get(HAS_LOCATION)
            querystring = f"https://api.mindat.org/localities/?txt={location.get('dwc:locality')}"
            response = requests.get(
                querystring,
                headers={"Authorization": "Token " + os.environ.get("API_KEY")},
            )
            logging.info("Response from mindat status code: " + str(response.status_code))
            logging.info("Response from mindat" + str(response.content))
            response_json = json.loads(response.content)
            if not response_json:
                logging.info("No results for this locality where found: " + querystring)
            else:
                logging.info("Highest hit is: " + json.dumps(response_json.get("results")[0], indent=2))
                result_list.append(
                    {
                        "queryString": querystring,
                        "geo_reference_result": response_json.get("results")[0],
                        "result_index": index,
                    }
                )
                if batching_requested:
                    batch_metadata.append(build_batch_metadata(event[HAS_LOCATION]["dwc:locality"], index))
    return result_list, batch_metadata


def run_local(example: str):
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/W32-FLA-P8V
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen_data = json.loads(response.content).get("data").get("attributes")
    result, batch_metadata = run_georeference(specimen_data, True)
    annotation_event = map_to_annotation_event(specimen_data, result, str(uuid.uuid4()), batch_metadata)
    logging.info("Created annotations: " + json.dumps(annotation_event, indent=2))


if __name__ == "__main__":
    run_rabbitmq()
    # run_local("https://dev.dissco.tech/api/digital-specimen/v1/TEST/RPK-51F-ZY6")
