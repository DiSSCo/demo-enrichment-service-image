import json
import logging
import os
import re
from typing import Dict, Any, List, Tuple
from requests.auth import HTTPBasicAuth

import pika
from jsonpath_ng import parse
import requests
from pika.amqp_object import Method, Properties
from pika.adapters.blocking_connection import BlockingChannel
import shared
import shared_ocr
from fuzzywuzzy import fuzz
import copy

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

"""
Restrictions: 
- Max 1 event
- Media associated with exactly one specimen
"""

DWC_MAPPING = {
    "dwc:catalogNumber": "['ods:hasIdentifiers'][*]['dcterms:identifier']",
    "dwc:recordNumber": "['ods:hasIdentifiers'][*]['dcterms:identifier']",
    "dwc:year": "$['ods:hasEvents'][*]['dwc:year']",
    "dwc:month": "$['ods:hasEvents'][*]['dwc:month']",
    "dwc:day": "$['ods:hasEvents'][*]['dwc:day']",
    "dwc:dateIdentified": "['ods:hasIdentifications'][*]['dwc:dateIdentified']",
    "dwc:verbatimIdentification": "['ods:hasIdentifications'][*]['dwc:verbatimIdentification']",
    "dwc:scientificName": "['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
    "dwc:decimalLatitude": "['ods:hasEvents'][*]['ods:hasLocation']['ods:hasGeoreference']['dwc:decimalLatitude']",
    "dwc:decimalLongitude": "['ods:hasEvents'][*]['ods:hasLocation']['ods:hasGeoreference']['dwc:decimalLongitude']",
    "dwc:locality": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:locality']",
    "dwc:minimumElevationInMeters": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:minimumElevationInMeters']",
    "dwc:maximumElevationInMeters": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:maximumElevationInMeters']",
    "dwc:verbatimElevation": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:verbatimElevation']",
    "dwc:country": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:country']",
    "dwc:countryCode": "['ods:hasEvents'][*]['ods:hasLocation']['dwc:countryCode']",
    "dwc:recordedBy": "['ods:hasIdentifications'][*]['ods:hasAgents'][*]['schema:name']",
    "dwc:identifiedBy": "['ods:hasIdentifications'][*]['ods:hasAgents'][*]['schema:name']",
}

FILTER_TERMS = {
    "dwc:catalogNumber": {
        "filter_class": "ods:hasIdentifiers",
        "target_field": "dcterms:title",
        "target_value": "dwc:catalogNumber",
        "array_field": False,
    },
    "dwc:recordedBy": {
        "parent_class": "ods:hasIdentifications",
        "filter_class": "ods:hasAgents",
        "target_field": "ods:hasRoles",
        "target_value": "recorder",
        "array_field": True,
    },
    "dwc:identifiedBy": {
        "parent_class": "ods:hasIdentifications",
        "filter_class": "ods:hasAgents",
        "target_field": "ods:hasRoles",
        "target_value": "identifier",
        "array_field": True,
    },
}

SIMILARITY_THRESHOLD = 50


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
        event = {"annotations": annotations, "jobId": json_value.get("jobId")}
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
    # Your query here
    query_string, uris = build_query_string(digital_media)

    timestamp = shared.timestamp_now()
    # Run API call and compute value(s) of the annotation
    response = run_api_call(query_string, uris)
    if not response:
        # If the API call does not return a result, that information should still be captured in an annotation
        return [
            shared.map_to_empty_annotation(
                timestamp,
                "Unable to read specimen label",
                digital_media[shared.ODS_ID],
                digital_media[shared.ODS_TYPE],
                query_string,
            )
        ]
    specimen = shared_ocr.get_specimen_from_media(digital_media)
    annotations = []

    return shared_ocr.map_ocr_response_to_annotations(
        annotations, query_string, response.get("data"), specimen, timestamp, DWC_MAPPING
    )


def compare_result_to_existing_info(specimen: Dict[str, Any], path: str, result_value) -> Tuple[str, str]:
    """
    Compares result value (from AI) to what was already in the specimen. Sets the annotation value and motivation accordingly
    :param specimen: the specimen
    :param path: the path of the value we're checking
    :param result_value: the value for this term
    :return: a tuple of (value, motivation)
    """
    path_value = get_value_at_path(path, specimen)
    if path_value == result_value:
        return "Existing information aligns with AI processing", shared.Motivation.ASSESSING.value
    return result_value, shared.Motivation.EDITING.value


def find_fuzzy_match(specimen: Dict[str, Any], paths: List[str], value: str) -> Tuple[str, str]:
    """
    Given multiple potential targets to annotate, finds the most appropriate one. Compares the result value to what is
    already in the specimen, and finds the closest match to annotate. If no match is close enough, we determine this is
    not an "editing" but an "assessing" motivation.
    :param specimen: the specimen
    :param paths: the paths of potential matches in the specimen
    :param value: the value from the AI model we're checking against
    :return: best path to annotate, motivation
    """
    # Multiple matches - use fuzzy matching
    best_match = None
    best_score = 0
    motivation = None
    for path in paths:
        # Get the value at the current path
        path_value = get_value_at_path(path, specimen)
        if path_value == value:  # For non-string values, use exact comparison
            # Exact matches always win
            best_match = path
            motivation = shared.Motivation.ASSESSING.value
            break  # No need to check further if we found an exact match
        elif isinstance(path_value, str) and isinstance(value, str):
            # Use fuzzy string matching for strings
            # Try both ratio and partial_ratio to handle different matching scenarios
            similarity = max(
                fuzz.ratio(path_value.lower(), value.lower()),
                fuzz.partial_ratio(path_value.lower(), value.lower()),
            )
            # Only update best match if this is better than current best
            if similarity > best_score and similarity >= SIMILARITY_THRESHOLD:  # Use parameterized threshold
                best_score = similarity
                best_match = path
                motivation = shared.Motivation.EDITING.value
    return (best_match, motivation) if best_match else ("", shared.Motivation.ADDING.value)


def get_value_at_path(path: str, specimen: Dict[str, Any]) -> str:
    path_expr = parse(path)
    return path_expr.find(specimen)[0].value


def append_new_information(specimen: Dict[str, Any], response_field, paths: List[str]) -> str:
    if response_field in FILTER_TERMS:
        paths = get_json_path(specimen, response_field, False)
    last_path = paths[-1]
    last_index = re.search(r"(\d+)(?!.*\d)", last_path).group(1)
    return re.sub("(\\d+)(?!.*\\d)", str(int(last_index) + 1), last_path)


def get_json_path(specimen: Dict[str, Any], field: str, do_filter: bool) -> List[str]:
    """
    Gets json path of desired field (and optional value)
    Returns path in block notation format by splitting on dots and adding square brackets
    Field path should be in block notation
    Returns: json path in block notation
    """
    if field in FILTER_TERMS and do_filter:
        obj = prune_specimen(specimen, FILTER_TERMS[field])
    else:
        obj = specimen
    field_path = DWC_MAPPING[field]
    path_expr = parse(field_path)
    matches = path_expr.find(obj)
    if matches:
        return to_block_notation(matches)
    return []


def prune_specimen(specimen: Dict[str, Any], filter_terms: Dict[str, Any]) -> Dict[str, Any]:
    """
    In some cases, we can't use every path for a given term. E.g. If we're looking to annotate the "collector"'s name,
    we need to identify the correct agent first.
    This function creates a copy of the specimen and removes all non-relevant paths.
    :param specimen: specimen to filter
    :param filter_terms: dictionary of filter terms, containing:
        filter_class: class to filter through, e.g. "ods:hasIdentifiers"
        target_field: field which determines if an item belongs in the filter_class, e.g. "dcterms:title"
        target_value: value to check in target_field, e.g. "dwc:catalogNumber"
    returns: pruned specimen
    """
    specimen_copy = copy.deepcopy(specimen)
    if filter_terms.get("parent_class"):
        prune_class = specimen_copy.get(filter_terms["parent_class"])
        for item in prune_class:
            prune_object(item, filter_terms)
        return specimen_copy
    else:
        return prune_object(specimen_copy, filter_terms)


def prune_object(object_copy: Dict[str, Any], filter_terms: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prunes a subset of the specimen
    object_copy: copy of the class we want to prune
    filter_terms: dictionary of filter terms
    returns: the filtered object, with non-relevant terms removed
    """
    remove_these = []
    filter_class = object_copy.get(filter_terms.get("filter_class"))
    if not filter_class:
        return object_copy
    for idx, item in enumerate(filter_class):
        if (
            filter_terms.get("array_field")
            and filter_terms.get("target_value") not in item[filter_terms.get("target_field")]
        ) or (
            not filter_terms.get("array_field")
            and item[filter_terms.get("target_field")] != filter_terms.get("target_value")
        ):
            remove_these.append(idx)
    for idx in remove_these:
        object_copy.get(filter_terms.get("filter_class"))[idx] = {}
    return object_copy


def to_block_notation(matches: Any) -> List[str]:
    """
    Converts a list of matches to strings in JSON path block notation (see DWC_MAPPING for examples)
    :param matches: list of matches from our JSON path parser
    :return: list of strings formatted in block notation
    """
    # Convert the first match to block notation string
    paths = []
    for match in matches:
        path = str(match.full_path)
        # Split on dots and format each part
        parts = path.split(".")
        formatted_parts = []
        for part in parts:
            if not part.startswith("["):
                # Remove any single quotes before wrapping in single quotes
                part = part.strip("'")
                part = f"['{part}']"
            formatted_parts.append(part)
        paths.append("".join(formatted_parts))
    return paths


def build_query_string(digital_object: Dict[str, Any]) -> Tuple[str, List[str]]:
    """
    Builds the query for n8n endpoint
    :param digital_object: Target of the annotation
    :return: query string to some example API, list of access URIs
    """
    access_uris = [digital_object.get("ac:accessURI")]
    # Use your API here
    return "https://n8n.svc.gbif.no/webhook/9fa39dd6-63ea-4ed8-b4e1-904051e8a41a", access_uris


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


def run_api_call(query_string: str, uris: List[str]) -> Dict[str, Any]:
    try:
        auth = HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD"))
        response = requests.post(query_string, json={"uris": uris}, auth=auth, timeout=60)
        response.raise_for_status()
        response_json = json.loads(response.content)
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException
    return response_json


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
    # run_local("SANDBOX/DTZ-A0R-TZ9")
