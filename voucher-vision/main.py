import json
import logging
import os
from typing import Dict, Any, List, Tuple

import pika
import requests
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel
from client import process_image, ordereddict_to_json
import shared
import shared_ocr

from shared.RequestFailedException import RequestFailedException

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

DWC_MAPPING = {
    "catalogNumber": "['ods:hasIdentifiers'][*]['dcterms:identifier']",
    "scientificName": "['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
    "genus": "['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:genus']",
    "specificEpithet": "['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:specificEpithet']",
    "scientificNameAuthorship": "['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificNameAuthorship']",
    "collectedBy": "['ods:hasIdentifications'][*]['ods:hasAgents'][*]['schema:name']",
    "identifiedBy": "['ods:hasIdentifications'][*]['ods:hasAgents'][*]['schema:name']",
    "identifiedDate": "['ods:hasIdentifications'][*]['dwc:dateIdentified']",
    "identifiedRemarks": "['ods:hasIdentifications'][*]['dwc:identificationRemarks']",
    "verbatimCollectionDate": "['ods:hasIdentifications'][*]['dwc:verbatimEventDate']",
    "collectionDate": "['ods:hasEvents'][*]['dwc:eventDate']",
    "habitat": "['ods:hasEvents'][*]['dwc:habitat']",
    "country": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:country']",
    "continent": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:continent']",
    "county": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:county']",
    "stateProvince": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:stateProvince']",
    "locality": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:locality']",
    "minimumElevationInMeters": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:minimumElevationInMeters']",
    "maximumElevationInMeters": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:maximumElevationInMeters']",
    "decimalLatitude": "['ods:hasEvents'][*]['ods:hasLocation']['ods:hasGeoreference']['dwc:decimalLatitude']",
    "decimalLongitude": "['ods:hasEvents'][*]['ods:hasLocation']['ods:hasGeoreference']['dwc:decimalLongitude']",
    "verbatimCoordinates": "['ods:hasEvents'][*]['ods:hasLocation']['ods:hasGeoreference']['dwc:verbatimCoordinates']",
}


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
        # Indicates to DiSSCo the message has been received by the mas and the job is running.
        # DiSSCo then informs the user of this development
        job_id = json_value.get("jobId")
        logging.info(f"Received job with jobId: {job_id}")
        shared.mark_job_as_running(job_id=job_id)
        digital_object = json_value.get("object")
        annotations = build_annotations(digital_object)
        event = {"annotations": annotations, "jobId": job_id}
        logging.info(f"Publishing annotation event: {json.dumps(event)}")
        publish_annotation_event(event, channel)
    except Exception as e:
        logging.error(f"Failed to publish annotation event: {e}")
        shared.send_failed_message(json_value.get("jobId"), str(e), channel)


def build_annotations(digital_media: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Given a target object, computes a result and maps the result to an openDS annotation
    :param digital_media: the target object of the annotation
    :return: List of annotations
    """
    query_string, uri = build_query_string(digital_media)

    timestamp = shared.timestamp_now()
    dcterms_ref, response = run_api_call(query_string, uri)
    if not response:
        return [
            shared.map_to_empty_annotation(
                timestamp,
                "Unable to read specimen label",
                digital_media[shared.ODS_ID],
                digital_media[shared.ODS_TYPE],
                dcterms_ref,
            )
        ]
    specimen = shared_ocr.get_specimen_from_media(digital_media)
    annotations = []

    return shared.map_ocr_response_to_annotations(annotations, dcterms_ref, response, specimen, timestamp, DWC_MAPPING)


def build_query_string(digital_object: Dict[str, Any]) -> Tuple[str, str]:
    """
    Retrieves the api endpoint and access URI from the digital object.
    :param digital_object: Target of the annotation
    :return: query string to some example API, the access URI of the image to be processed
    """
    access_uri = digital_object.get("ac:accessURI")
    return os.environ.get("API_ENDPOINT", "https://vouchervision-go-738307415303.us-central1.run.app/"), access_uri


def publish_annotation_event(annotation_event: Dict[str, Any], channel: BlockingChannel) -> None:
    """
    Send the annotation to the RabbitMQ queue
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


def run_api_call(query: str, access_uri: str) -> Tuple[str, Dict[str, Any]]:
    """
    Runs the API call to the Voucher Vision service and returns the result.
    We do not use the fileName or output directory here as we wrap the response to an annotation directly.
    :param query: endpoint of the API from running voucher vision
    :param access_uri: The access URI of the image to be processed
    :return: A reference to the API call including the engine and prompt used, and the response from the API (but only the formatted JSON result)
    """
    auth_token = os.environ.get("API_KEY")  # Add auth token as an environment variable or secret
    engine = ["gemini-2.0-flash"]
    prompt = "SLTPvM_default_v2.yaml"
    result = process_image(
        fname=None,
        server_url=query,
        image_path=access_uri,
        output_dir=None,
        verbose=True,
        engines=engine,
        prompt=prompt,
        auth_token=auth_token,
    )
    if not result:
        raise RequestFailedException("No result returned from the API call")
    output_str = ordereddict_to_json(result, output_type="dict")
    return f"endpoint:{query}| engine:{engine} | prompt:{prompt}", output_str.get("formatted_json")


def run_local(media_id: str):
    """
    Runs script locally. Demonstrates using a specimen target
    :param media_id: A media ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_media = (
        requests.get(f"https://sandbox.dissco.tech/api/digital-media/v1/{media_id}")
        .json()
        .get("data")
        .get("attributes")
    )
    specimen_annotations = build_annotations(digital_media)
    event = {"annotations": specimen_annotations, "jobId": "Some job ID"}
    logging.info(f"created annotation event: {json.dumps(event)}")


if __name__ == "__main__":
    run_rabbitmq()
    # run_local("SANDBOX/KWL-6C2-WDG")
