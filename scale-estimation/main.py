import json
import logging
import os
import uuid
import requests
import pika
import shared
from typing import Tuple, Any, Dict, List, Optional
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

SCALE_ESTIMATION_URL = "https://herbaria.idlab.ugent.be/api/mas-scale-estimation"
METHOD_REFERENCE = "https://github.com/kymillev/demo-enrichment-service-image"


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
        image_uri = get_image_uri(digital_object)
        scale, image_height, image_width = run_scale_estimation(image_uri)
        annotations = map_result_to_annotation(digital_object, scale, image_height, image_width)
        annotation_event = map_to_annotation_event(annotations, json_value["jobId"])

        logging.info(f"Publishing annotation event: {json.dumps(annotation_event)}")
        publish_annotation_event(annotation_event, channel)
    except Exception as e:
        shared.send_failed_message(json_value["jobId"], str(e), channel)


def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {"annotations": annotations, "jobId": job_id}


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


def map_to_assertion(scale: Dict[str, Any], timestamp: str, ods_agent: Dict) -> Dict[str, Any]:
    """
    Map the scale factor of the best ruler match to an openDS Assertion.
    :param scale: parsed best match from the scale estimation service
    :param timestamp: timestamp of the measurement
    :param ods_agent: MAS as agent object
    :return: ods:Assertion object
    """
    remarks = (
        f"Overall score: {scale.get('overall_score')}. "
        f"Image similarity: {scale.get('image_similarity')}. "
        f"Scale factor unit: {scale.get('scale_factor_unit')}."
    )
    return {
        shared.AT_TYPE: "ods:Assertion",
        "dwc:measurementType": "scale factor",
        "dwc:measurementValue": str(scale.get("scale_factor")),
        "dwc:measurementUnit": scale.get("scale_factor_unit"),
        "dwc:measurementDeterminedDate": timestamp,
        "dwc:measurementMethod": f"Scale estimated with {scale.get('method')}",
        "dwc:measurementRemarks": remarks,
        "ods:hasAgents": [ods_agent],
    }


def map_result_to_annotation(
    digital_object: Dict,
    scale: Optional[Dict[str, Any]],
    image_height: int,
    image_width: int,
) -> List[Dict[str, Any]]:
    """
    Map the best ruler match to annotations:
    - one on the ruler region, so the bounding box is shown on the image
    - one adding the scale factor as an ods:Assertion
    - one adding ac:scaleFactor and one adding ac:scaleFactorUnits, the terms proposed in
      https://github.com/tdwg/ac/issues/283
    If no ruler match was returned, a comment annotation is returned instead.
    :param digital_object: the target object of the annotation
    :param scale: parsed best match, or None when the service found no ruler
    :param image_height: height of the image in pixels
    :param image_width: width of the image in pixels
    :return: list of annotations
    """
    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()

    if not scale:
        return [map_result_to_empty_annotation(digital_object, image_height=image_height, image_width=image_width)]

    annotations = []
    region_value = {
        "boundingBox": scale.get("boundingBox"),
        "class": scale.get("class"),
        "score": scale.get("score"),
        "scale_factor": scale.get("scale_factor"),
        "scale_factor_unit": scale.get("scale_factor_unit"),
        "overall_score": scale.get("overall_score"),
        "image_similarity": scale.get("image_similarity"),
    }
    region_selector = shared.build_fragment_selector(scale, image_width, image_height)
    annotations.append(map_scale_annotation(digital_object, scale, region_value, region_selector, timestamp))

    assertion_index = len(digital_object.get("ods:hasAssertions", []))
    assertion_value = map_to_assertion(scale, timestamp, ods_agent)
    assertion_selector = shared.build_class_selector(f"$['ods:hasAssertions'][{assertion_index}]")
    annotations.append(map_scale_annotation(digital_object, scale, assertion_value, assertion_selector, timestamp))

    scale_factor_selector = shared.build_term_selector("$['ac:scaleFactor']")
    annotations.append(
        map_scale_annotation(
            digital_object,
            scale,
            {"ac:scaleFactor": scale.get("scale_factor")},
            scale_factor_selector,
            timestamp,
        )
    )
    scale_unit_selector = shared.build_term_selector("$['ac:scaleFactorUnits']")
    annotations.append(
        map_scale_annotation(
            digital_object,
            scale,
            {"ac:scaleFactorUnits": scale.get("scale_factor_unit")},
            scale_unit_selector,
            timestamp,
        )
    )
    return annotations


def map_scale_annotation(
    digital_object: Dict, scale: Dict[str, Any], oa_value: Dict, oa_selector: Dict, timestamp: str
) -> Dict[str, Any]:
    """
    Build an annotation on the target object with the overall score of the best ruler match.
    :param digital_object: the target object of the annotation
    :param scale: parsed best match from the scale estimation service
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


def map_result_to_empty_annotation(digital_object: Dict, image_height: int, image_width: int):
    """
    Given a target object and no found detections, map the result to an openDS comment annotation to inform the user.
    :param digital_object: the target object of the annotation
    :param image_height: the height of the processed image
    :param image_width: the width of the processed image
    :return: Annotation event
    """
    timestamp = shared.timestamp_now()
    message = "Scale estimation model found no detections in this image"
    selector = shared.build_entire_image_fragment_selector(height=image_height, width=image_width)

    annotation = shared.map_to_annotation_str_val(
        ods_agent=shared.get_agent(),
        timestamp=timestamp,
        oa_value=message,
        oa_selector=selector,
        target_id=digital_object[shared.ODS_ID],
        target_type=digital_object[shared.ODS_TYPE],
        dcterms_ref=METHOD_REFERENCE,
        motivation="oa:commenting",
    )

    return annotation


def get_image_uri(digital_object: Dict, source_url: str | None = None) -> str:
    """
    Resolve the herbarium sheet image URL from a digital media object or a digital specimen.
    Specimens do not carry ac:accessURI. Their images are linked through
    ods:hasEntityRelationships where dwc:relationshipOfResource is hasDigitalMedia.
    Those digital media records are fetched, and the StillImage access URI is returned.
    :param digital_object: openDS digital specimen or digital media attributes
    :param source_url: API URL the specimen was loaded from, used to find the matching media API
    :return: image access URI
    """
    media_type = digital_object.get("dcterms:type")
    access_uri = digital_object.get("ac:accessURI")
    if access_uri and (media_type in (None, "StillImage") or str(digital_object.get("dcterms:format", "")).startswith("image/")):
        return access_uri

    media_ids = [
        relationship.get("dwc:relatedResourceID")
        for relationship in digital_object.get("ods:hasEntityRelationships", [])
        if relationship.get("dwc:relationshipOfResource") == "hasDigitalMedia"
        and relationship.get("dwc:relatedResourceID")
    ]
    if not media_ids:
        raise ValueError("No image access URI or linked digital media found on the digital object")

    api_base = _dissco_api_base(digital_object, source_url)
    for media_id in media_ids:
        media_response = requests.get(f"{api_base}/api/digital-media/v1/{media_id}", timeout=30)
        media_response.raise_for_status()
        media = media_response.json().get("data", {}).get("attributes", {})
        uri = media.get("ac:accessURI")
        if not uri:
            continue
        if media.get("dcterms:type") == "StillImage" or str(media.get("dcterms:format", "")).startswith("image/"):
            return uri

    raise ValueError("Linked digital media did not include a StillImage access URI")


def _dissco_api_base(digital_object: Dict, source_url: str | None = None) -> str:
    """
    Return the DiSSCo API host for digital media lookups.
    :param digital_object: openDS object, used when no source URL is available
    :param source_url: specimen or media API URL
    :return: API origin, for example https://sandbox.dissco.tech
    """
    if source_url and "/api/" in source_url:
        return source_url.split("/api/")[0]

    identifier = digital_object.get("dcterms:identifier") or digital_object.get("@id") or ""
    prefix = identifier.rstrip("/").split("/")[-2] if "/" in identifier else ""
    hosts = {
        "SANDBOX": "https://sandbox.dissco.tech",
        "TEST": "https://dev.dissco.tech",
    }
    return hosts.get(prefix, "https://sandbox.dissco.tech")


def parse_scale_estimation_response(response_json: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], int, int]:
    """
    Read the scale factor and the first match result from the scale estimation response.
    The first entry in match_results is the best match.
    :param response_json: JSON body returned by the scale estimation API
    :return: parsed ruler match, image height, and image width. The match is None when no ruler was found.
    """
    img_shape = response_json.get("metadata", {}).get("image_shape") or [0, 0]
    img_height, img_width = img_shape[:2]

    match_results = response_json.get("match_results") or []
    if not match_results:
        return None, img_height, img_width

    best_match = match_results[0]
    ruler_bbox = best_match.get("ruler_bbox_xyxy")
    if not ruler_bbox:
        return None, img_height, img_width

    scale_factor_unit = best_match.get("scale_factor_unit", best_match.get("scale_factor_units"))
    scale = {
        "boundingBox": ruler_bbox,
        "class": "ruler",
        "score": best_match.get("overall_score"),
        "overall_score": best_match.get("overall_score"),
        "image_similarity": best_match.get("image_similarity"),
        "scale_factor": response_json.get("scale_factor", best_match.get("scale_factor")),
        "scale_factor_unit": scale_factor_unit,
        "method": best_match.get("method"),
    }
    return scale, img_height, img_width


def run_scale_estimation(image_uri: str) -> Tuple[Optional[Dict[str, Any]], int, int]:
    """
    Makes an API request to the scale estimation service hosted at IDLab.
    :param image_uri: The URI of the image to be processed
    :return: parsed best ruler match, the processed image height, and the processed image width
    """
    payload = {"image_url": image_uri}

    headers = {"Content-Type": "application/json"}
    response = requests.post(SCALE_ESTIMATION_URL, json=payload, headers=headers)

    response.raise_for_status()
    return parse_scale_estimation_response(response.json())


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the rabbitmq consumer with this method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://sandbox.dissco.tech/api/digital-specimen/v1/SANDBOX/BAC-9KZ-T77
    :return: Return nothing but will log the result and write temp_output.json
    """
    response = requests.get(example)
    json_value = json.loads(response.content).get("data")

    digital_object = json_value.get("attributes")
    image_uri = get_image_uri(digital_object, source_url=example)
    logging.info(f"Image URI: {image_uri}")
    scale, image_height, image_width = run_scale_estimation(image_uri)
    annotations = map_result_to_annotation(digital_object, scale, image_height, image_width)
    annotation_event = map_to_annotation_event(annotations, str(uuid.uuid4()))

    pretty_output = json.dumps(annotation_event, indent=2)
    logging.info("Created annotations:\n" + pretty_output)
    with open("temp_output.json", "w", encoding="utf-8") as output_file:
        output_file.write(pretty_output + "\n")


if __name__ == "__main__":
    # Local testing
    specimen_url = "https://sandbox.dissco.tech/api/digital-specimen/v1/SANDBOX/BAC-9KZ-T77"
    run_local(specimen_url)
    #run_rabbitmq()
