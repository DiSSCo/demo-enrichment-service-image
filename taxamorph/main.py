import json
import logging
import os
from typing import Dict, List, Any
import requests
import pika
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel
import requests_cache
import shared
import uuid


# Enable caching for both GET and POST requests
requests_cache.install_cache(
    "taxamorph",
    expire_after=3600,  # Cache for 1 hour
    allowable_methods=["GET", "POST"],  # Include POST requests in the cache
)

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

TAXAMORPH_ENDPOINT = "https://merry-malamute-bold.ngrok-free.app/infer"


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
    Callback function to process the message from RabbitMQ.
    This method will be called for each message received.
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
        digital_media = json_value.get("object")
        taxamorph_result = run_api_call(digital_media)
        annotations = map_result_to_annotation(digital_media, taxamorph_result)
        event = map_to_annotation_event(annotations, json_value.get("jobId"))
        publish_annotation_event(event, channel)
    except Exception as e:
        send_failed_message(json_value.get("jobId"), str(e), channel)


def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {"annotations": annotations, "jobId": job_id}


def map_result_to_annotation(
    digital_media: Dict,
    taxamorph_result: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Map the result of the API call to an annotation
    :param digital_media: the digital media object containing the ODS ID and type
    :param taxamorph_result: Result of the TaxaMorph API call
    :return:
    """

    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()

    if taxamorph_result:
        annotations = list()

        for result in taxamorph_result:
            oa_value = shared.map_to_entity_relationship(
                "hasTaxaMorphDownloadURL",
                result["downloadURL"],
                result["downloadURL"],
                timestamp,
                ods_agent,
            )

            oa_selector = shared.build_class_selector(shared.ER_PATH)

            annotation = shared.map_to_annotation(
                ods_agent,
                timestamp,
                oa_value,
                oa_selector,
                digital_media[shared.ODS_ID],
                digital_media[shared.ODS_TYPE],
                result["downloadURL"],
            )
            annotations.append(annotation)
    else:
        annotations = [
            shared.map_to_empty_annotation(timestamp, "No results for TaxaMorph", digital_media, shared.ER_PATH)
        ]

    return annotations


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


def run_api_call(digital_media: Dict) -> List[Dict[str, str]]:
    data = {
        "jobId": "20.5000.1025/AAA-111-BBB",
        "object": {
            "digitalSpecimen": {
                "@id": "https://doi.org/10.3535/XYZ-XYZ-XYZ",
                "dwc:scientificName": "Example species",
                "dwc:taxonID": "123456",
                "media": [digital_media],
            }
        },
        "batchingRequested": False,
    }

    try:
        response = requests.post(TAXAMORPH_ENDPOINT, json=data)
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return []

    annotations = result.get("annotations", [])
    if not annotations:
        logging.error("No annotations found in the response.")
        return []

    download_url = annotations[0].get("oa:hasBody", {}).get("oa:value")

    if not download_url:
        logging.error("No download URL found in the response.")
        return []

    logging.debug(f"Download image url:\n{download_url}")

    return [{"downloadURL": download_url, "processid": "TaxaMorph"}]


def run_local(media_id: str) -> None:
    """
    Runs script locally. Demonstrates using a specimen target
    :param specimen_id: A specimen ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_media = (
        requests.get(f"https://sandbox.dissco.tech/api/digital-media/v1/{media_id}")
        .json()
        .get("data")
        .get("attributes")
    )

    taxamorph_result = run_api_call(digital_media)

    annotations = map_result_to_annotation(digital_media, taxamorph_result)

    event = map_to_annotation_event(annotations, str(uuid.uuid4()))

    logging.info("Created annotations: " + json.dumps(event, indent=2))


if __name__ == "__main__":
    # run_local('SANDBOX/4LB-38S-KSM')
    run_rabbitmq()
