import json
import logging
import os
import uuid
from typing import Any

import pika
import requests
from pika.adapters.blocking_connection import BlockingChannel
from pika.amqp_object import Method, Properties

import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

SCALE_DETECTION_URL = os.environ.get(
    "SCALE_DETECTION_URL", "https://webapp.senckenberg.de/dissco-ruler-detection-mas-proto/scale_detection"
)
METHOD_REFERENCE = "https://github.com/DiSSCo/demo-enrichment-service-image/tree/main/ruler_detection"


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
    logger.info(f"Received message: {json_value!s}")
    try:
        shared.mark_job_as_running(job_id=json_value.get("jobId"))
        digital_object = json_value.get("object")
        scales, image_height, image_width = run_scale_detection(digital_object.get("ac:accessURI"))
        annotations = map_result_to_annotation(digital_object, scales, image_height, image_width)
        annotation_event = map_to_annotation_event(annotations, json_value["jobId"])

        logger.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
        publish_annotation_event(annotation_event, channel)
    except Exception as e:  # noqa: BLE001 any failure must be reported to DiSSCo as a failed job
        logger.error(f"Failed to publish annotation event: {e}")
        shared.send_failed_message(json_value["jobId"], str(e), channel)


def map_to_annotation_event(annotations: list[dict], job_id: str) -> dict:
    return {"annotations": annotations, "jobId": job_id}


def publish_annotation_event(annotation_event: dict, channel: BlockingChannel) -> None:
    """
    Send the annotation to the RabbitMQ queue
    :param annotation_event: The formatted annotation event
    :param channel: A RabbitMQ BlockingChannel to which we will publish the annotation
    :return: Will not return anything
    """
    logger.info("Publishing annotation: " + str(annotation_event))
    channel.basic_publish(
        exchange=os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-exchange"),
        routing_key=os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation"),
        body=json.dumps(annotation_event).encode("utf-8"),
    )


def map_to_assertion(scale: dict[str, Any], timestamp: str, ods_agent: dict) -> dict[str, Any]:
    """
    Map one scale reading to an openDS Assertion (dwc:MeasurementOrFact).
    :param scale: one detected scale from the scale detection service
    :param timestamp: timestamp of the measurement
    :param ods_agent: MAS as agent object
    :return: ods:Assertion object
    """
    remarks = (
        f"Unit printed on the scale: {scale.get('unit')}. "
        f"Numbers read on the scale: {scale.get('numbers_read')}. "
        f"Tick period in pixel: {scale.get('tick_period_in_pixel')}. "
        f"Verified by tick pattern and sheet size: {scale.get('verified')}."
    )
    return {
        shared.AT_TYPE: "ods:Assertion",
        "dwc:measurementType": "pixels per centimetre",
        "dwc:measurementValue": str(scale.get("one_cm_in_pixel")),
        "dwc:measurementUnit": "px/cm",
        "dwc:measurementDeterminedDate": timestamp,
        "dwc:measurementMethod": f"Scale detected with {scale.get('method')} and read with Tesseract OCR",
        "dwc:measurementRemarks": remarks,
        "ods:hasAgents": [ods_agent],
    }


def map_result_to_annotation(
    digital_object: dict,
    scales: list[dict[str, Any]],
    image_height: int,
    image_width: int,
) -> list[dict[str, Any]]:
    """
    Map each verified scale reading to two annotations:
    - one on the region of the image where the scale is, so the bounding box is shown on the image
    - one adding the pixel per cm reading as an ods:Assertion to the media object
    If no scale could be read, a comment annotation is returned instead.
    :param digital_object: the target object of the annotation
    :param scales: detected scales from the scale detection service
    :param image_height: height of the image in pixels
    :param image_width: width of the image in pixels
    :return: list of annotations
    """
    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()
    # Only verified readings are annotated, unverified ones are often misread numbers
    verified_scales = [scale for scale in scales if scale.get("verified")]

    if not verified_scales:
        return [
            shared.map_to_empty_annotation(
                timestamp,
                "No verified scale reading found on the image",
                digital_object,
                "$['ac:accessURI']",
                METHOD_REFERENCE,
            )
        ]

    annotations = []
    # New assertions are added after the ones the media object already has
    assertion_index = len(digital_object.get("ods:hasAssertions", []))
    for scale in verified_scales:
        region_value = {
            "boundingBox": scale.get("boundingBox"),
            "class": scale.get("class"),
            "score": scale.get("score"),
            "one_cm_in_pixel": scale.get("one_cm_in_pixel"),
            "unit": scale.get("unit"),
            "verified": scale.get("verified"),
        }
        region_selector = shared.build_fragment_selector(scale, image_width, image_height)
        annotations.append(map_scale_annotation(digital_object, scale, region_value, region_selector, timestamp))

        assertion_value = map_to_assertion(scale, timestamp, ods_agent)
        assertion_selector = shared.build_class_selector(f"$['ods:hasAssertions'][{assertion_index}]")
        annotations.append(map_scale_annotation(digital_object, scale, assertion_value, assertion_selector, timestamp))
        assertion_index += 1

    return annotations


def map_scale_annotation(
    digital_object: dict, scale: dict[str, Any], oa_value: dict, oa_selector: dict, timestamp: str
) -> dict[str, Any]:
    """
    Build an annotation on the media object with the detection score of the scale.
    :param digital_object: the target object of the annotation
    :param scale: detected scale from the scale detection service
    :param oa_value: value of the annotation
    :param oa_selector: selector of the annotation
    :param timestamp: timestamp of the annotation
    :return: formatted annotation
    """
    annotation = shared.map_to_annotation(
        shared.get_agent(),
        timestamp,
        oa_value,
        oa_selector,
        digital_object[shared.ODS_ID],
        digital_object[shared.ODS_TYPE],
        METHOD_REFERENCE,
    )
    annotation["oa:hasBody"]["ods:score"] = round(scale.get("score"), 3)
    return annotation


def run_scale_detection(image_uri: str) -> tuple[list[dict[str, Any]], int, int]:
    """
    Post the image url to the scale detection service.
    :param image_uri: The image url of the herbarium sheet
    :return: list of detected scales, image height and image width
    """
    response = requests.post(
        SCALE_DETECTION_URL,
        auth=(os.environ.get("SCALE_DETECTION_USER"), os.environ.get("SCALE_DETECTION_PASSWORD")),
        json={"image_url": image_uri},
        timeout=60,
    )
    response.raise_for_status()
    response_json = response.json()
    scales = response_json.get("output", [])
    if not scales:
        logger.info("No scale found on this herbarium sheet: " + image_uri)
    return scales, response_json.get("image_height"), response_json.get("image_width")


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the rabbitmq consumer with this method call in the main method.
    Will call the DiSSCo API to retrieve the media data.
    :param example: The full URL of the Digital Media to the API (for example
    https://sandbox.dissco.tech/api/digital-media/v1/SANDBOX/TC9-7ER-QVP)
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    digital_object = json.loads(response.content).get("data").get("attributes")
    scales, image_height, image_width = run_scale_detection(digital_object.get("ac:accessURI"))
    annotations = map_result_to_annotation(digital_object, scales, image_height, image_width)

    event = map_to_annotation_event(annotations, str(uuid.uuid4()))
    logger.info("Created annotations: " + json.dumps(event))


if __name__ == "__main__":
    run_rabbitmq()
    # run_local("https://sandbox.dissco.tech/api/digital-media/v1/SANDBOX/TC9-7ER-QVP")
