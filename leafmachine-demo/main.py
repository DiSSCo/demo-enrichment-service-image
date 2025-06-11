import json
import logging
import os
import uuid
import requests
import pika
import shared
from typing import Tuple, Any, Dict, List
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
        shared.mark_job_as_running(job_id=json_value.get("jobId"))
        digital_object = json_value.get("object")
        image_uri = digital_object.get("ac:accessURI")
        additional_info_annotations, image_height, image_width = run_leafmachine(image_uri)

        # Publish an annotation comment if no plant components were found
        if len(additional_info_annotations) == 0:
            logging.info(f"No results for this herbarium sheet: {image_uri} - jobId: {json_value['jobId']}")
            annotation = map_result_to_empty_annotation(
                digital_object, image_height=image_height, image_width=image_width
            )

            annotation_event = map_to_annotation_event([annotation], json_value["jobId"])
        # Publish the annotations if plant components were found
        else:
            annotations = map_result_to_annotation(
                digital_object, additional_info_annotations, image_height=image_height, image_width=image_width
            )
            annotation_event = map_to_annotation_event(annotations, json_value["jobId"])

        logging.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
        publish_annotation_event(annotation_event, channel)
    except Exception as e:
        send_failed_message(json_value["jobId"], str(e), channel)


def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {"annotations": annotations, "jobId": job_id}


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


def map_result_to_annotation(
    digital_object: Dict,
    additional_info_annotations: List[Dict[str, Any]],
    image_height: int,
    image_width: int,
):
    """
    Given a target object, computes a result and maps the result to an openDS annotation.
    :param digital_object: the target object of the annotation
    :param additional_info_annotations: the result of the computation
    :param image_height: the height of the processed image
    :param image_width: the width of the processed image
    :return: List of annotations
    """
    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()
    annotations = list()

    for annotation in additional_info_annotations:
        oa_value = annotation
        oa_selector = shared.build_fragment_selector(annotation, image_width, image_height)
        annotation = shared.map_to_annotation(
            ods_agent,
            timestamp,
            oa_value,
            oa_selector,
            digital_object[shared.ODS_ID],
            digital_object[shared.ODS_TYPE],
            "https://github.com/kymillev/demo-enrichment-service-image",
        )
        annotations.append(annotation)

    return annotations


def map_result_to_empty_annotation(digital_object: Dict, image_height: int, image_width: int):
    """
    Given a target object and no found plant components, map the result to an openDS comment annotation to inform the user.
    :param digital_object: the target object of the annotation

    :return: Annotation event
    """
    timestamp = shared.timestamp_now()
    message = "Leafpriority model found no plant components in this image"
    selector = shared.build_entire_image_fragment_selector(height=image_height, width=image_width)

    annotation = shared.map_to_annotation_str_val(
        ods_agent=shared.get_agent(),
        timestamp=timestamp,
        oa_value=message,
        oa_selector=selector,
        target_id=digital_object[shared.ODS_ID],
        target_type=digital_object[shared.ODS_TYPE],
        dcterms_ref="",
        motivation="oa:commenting",
    )

    return annotation


def run_leafmachine(image_uri: str, model_name: str = "leafpriority") -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Makes an API request to the LeafMachine backend service hosted at IDLab.
    :param image_uri: The URI of the image to be processed
    :param model_name: The name of the model used for inference
    :return: Returns a list of detected plant components, the processed image height, the processed image width
    """
    # Create the payload with image url and model name
    payload = {"image_url": image_uri, "model_name": model_name}

    # Send POST request to the IDLab server
    server_url = "https://herbaria.idlab.ugent.be/inference/process_image/"
    headers = {"Content-Type": "application/json"}
    response = requests.post(server_url, json=payload, headers=headers)

    response.raise_for_status()
    response_json = response.json()

    detections = response_json.get("detections", [])

    img_shape = response_json["metadata"]["orig_img_shape"]
    img_height, img_width = img_shape[:2]

    annotations_list = [
        {"boundingBox": det.get("bbox"), "class": det.get("class_name"), "score": det.get("confidence")}
        for det in detections
    ]

    return annotations_list, img_height, img_width


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


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/digital-media/TEST/GG9-1WB-N90
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    json_value = json.loads(response.content).get("data")

    digital_object = json_value.get("attributes")
    image_uri = digital_object.get("ac:accessURI")
    additional_info_annotations, image_height, image_width = run_leafmachine(image_uri)
    # additional_info_annotations = []
    # Publish an annotation comment if no plant components were found
    if len(additional_info_annotations) == 0:
        logging.info(f"No results for this herbarium sheet: {image_uri}")
        annotation = map_result_to_empty_annotation(digital_object, image_height=image_height, image_width=image_width)
        annotation_event = map_to_annotation_event([annotation], str(uuid.uuid4()))
    # Publish the annotations if plant components were found
    else:
        annotations = map_result_to_annotation(
            digital_object, additional_info_annotations, image_height=image_height, image_width=image_width
        )
        annotation_event = map_to_annotation_event(annotations, str(uuid.uuid4()))

    logging.info("Created annotations: " + json.dumps(annotation_event))


if __name__ == "__main__":
    # Local testing
    # specimen_url = "https://sandbox.dissco.tech/api/digital-media/v1/SANDBOX/TC9-7ER-QVP"
    # run_local(specimen_url)
    run_rabbitmq()
