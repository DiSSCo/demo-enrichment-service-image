import datetime
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
from shared_ocr.shared_ocr import get_specimen_from_media

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

"""
Restrictions:
- Max 1 event at the time, so no batch processing.
- Media associated with exactly one specimen. One specimen is allowed to have multiple medias(images).
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
        shared.mark_job_as_running(json_value.get("jobId"))
        specimen_data = json_value.get("object")
        result = run_api_call(specimen_data)
        annotation_event = map_to_annotation_event(specimen_data, result, json_value.get("jobId"))
        publish_annotation_event(annotation_event, channel)
    except Exception as e:
        shared.send_failed_message(json_value.get("jobId"), str(e), channel)


def map_to_annotation_event(media_object: Dict, result: Dict[str, str], job_id: str) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result which contains either 
        {
            "api_call": str,
            "scientific_name": str,
            "probability": str,
            "datetime": str,
            "api_tag": str
        } or {"error_message": str}
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
                    result.get("error_message", ""),
                    media_object,
                    shared.ER_PATH,
                    result.get("api_call", ""),
                )
            ],
        }
    ods_agent = shared.get_agent()
    
    specimen_object = get_specimen_from_media(media_object)
    
    
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
        media_object[shared.ODS_ID],
        media_object[shared.ODS_TYPE],
        result["queryString"],
    )

    return {"jobId": job_id, "annotations": [annotation]}


def publish_annotation_event(annotation_event: Dict, channel: BlockingChannel) -> None:
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


def run_api_call(media_object: Dict) -> Dict[str, str]:
    """
    Calls GBIF API based on the occurrenceID, catalogNumber and basisOfRecord
    :param media_object: The JSON data of the Media Object
    :return: The result from the API, which contains either the GBIF ID or an error message
    """
    image_path = get_image_from_object(media_object)
    if not image_path:
        logging.info("No relevant image found for specimen")
        return {
            "api_call": "",
            "error_message": "Failed to make the match, no relevant identifiers for GBIF",
        }
    identify_api_path = os.environ.get("IDENTIFY_API_PATH")
    if not identify_api_path:
        logging.error("IDENTIFY_API_PATH environment variable is not set")
        return {
            "api_call": "",
            "error_message": "Failed to make the match, API path not configured",
        }
    with open(image_path, "rb") as image:
        response = requests.post(identify_api_path, params={"image": image})
    response_json = json.loads(response.json())
    
    prediction_items = response_json["predictions"][0]["taxa"]["items"][0]
    generated_by = response_json["generated_by"]
    
    probability_top1 = prediction_items[0]["probability"]
    scientific_name_top1 = prediction_items[0]["scientificName"]
    datetime = generated_by["datetime"]
    api_tag = generated_by["tag"]
    
    return {
        "api_call": f'api: {identify_api_path}, image: {image_path}',
        "scientific_name": scientific_name_top1,
        "probability": probability_top1,
        "datetime": datetime,
        "api_tag": api_tag
    }

def get_image_from_object(media_object) -> str:
    print(media_object)
    imageUrl=media_object.get("ac:accessURI")
    return imageUrl

def get_image_type(media_object) -> str:
    serviceAccessPoints=media_object.get("serviceAccessPoints")
    imageFormat=serviceAccessPoints.get("format")
    return imageFormat
    


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the rabbitmq consumer with this method call in the main method.
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
    # run_rabbitmq()
    run_local("https://sandbox.dissco.tech/api/digital-media/v1/SANDBOX/53L-8ZD-C80")
