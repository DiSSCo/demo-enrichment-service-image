from datetime import datetime, timezone
from typing import Dict, Any
import json
import os
import requests
from enum import Enum
from pika.adapters.blocking_connection import BlockingChannel
import logging

ODS_TYPE = "ods:fdoType"
AT_TYPE = "@type"
ODS_ID = "dcterms:identifier"
AT_ID = "@id"
ER_PATH = "$['ods:hasEntityRelationships']"

MAS_ID = os.environ.get("MAS_ID")
MAS_NAME = os.environ.get("MAS_NAME")

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


class Motivation(Enum):
    ADDING = "ods:adding"
    ASSESSING = "oa:assessing"
    EDITING = "oa:editing"
    DELETING = "ods:deleting"
    COMMENTING = "oa:commenting"


def timestamp_now() -> str:
    """
    Create a timestamp in the correct format
    :return: The timestamp as a string
    """
    timestamp = str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + "Z"
    return timestamp_timezone


def mark_job_as_running(job_id: str):
    """
    Calls DiSSCo's RUNNING endpoint to inform the system that the message has
    been received by the MAS. Doing so will update the status of the job to
    "RUNNING" for any observing users.
    :param job_id: the job id from the message
    """
    query_string = f"{os.environ.get('RUNNING_ENDPOINT')}/{MAS_ID}/{job_id}/running"
    requests.get(query_string)


def get_agent() -> Dict[str, Any]:
    """
    Builds agent based on MAS ID and name
    :return: Agent object
    """
    return {
        AT_ID: f"https://hdl.handle.net/{MAS_ID}",
        AT_TYPE: "schema:SoftwareApplication",
        "schema:identifier": f"https://hdl.handle.net/{MAS_ID}",
        "schema:name": MAS_NAME,
        "ods:hasRoles": [
            {
                AT_TYPE: "schema:Role",
                "schema:roleName": "machine-annotation-service",
            }
        ],
        "ods:hasIdentifiers": [
            {
                AT_ID: f"https://hdl.handle.net/{MAS_ID}",
                AT_TYPE: "ods:Identifier",
                "dcterms:type": "Handle",
                "dcterms:title": "Handle",
                "dcterms:identifier": f"https://hdl.handle.net/{MAS_ID}",
                "ods:isPartOfLabel": False,
                "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                "ods:identifierStatus": "Preferred",
            }
        ],
    }


def map_to_entity_relationship(
    relationship_type: str,
    resource_id: str,
    resource_uri: str,
    timestamp: str,
    ods_agent: Dict,
) -> Dict:
    """
    :param relationship_type: Maps to dwc:relationshipOfResource
    :param resource_id: Id of related resource, maps to dwc:relatedResourceID
    :param resource_uri: Full URI of related resource
    :param timestamp: timestamp of ER creation
    :param ods_agent: MAS as agent object
    :return: formatted Entity relationship annotation
    """
    return {
        AT_TYPE: "ods:EntityRelationship",
        "dwc:relationshipOfResource": relationship_type,
        "dwc:relatedResourceID": resource_id,
        "ods:relatedResourceURI": resource_uri,
        "dwc:relationshipEstablishedDate": timestamp,
        "ods:hasAgents": [ods_agent],
    }


def map_to_empty_annotation(
    timestamp: str,
    message: str,
    target_data: Dict[str, Any],
    selector: str,
    dcterms_ref: str = "",
) -> Dict[str, Any]:
    """
    Returns an annotation for when no linkages for a given source were found
    :param timestamp: A formatted timestamp of the current time
    :param message: no results message
    :param target_data: Dict of the target data
    :param selector: Target class/term
    :param dcterms_ref: dcterms:ref value (value of the API call).
    :return: formatted annotation
    """
    return map_to_annotation_str_val(
        get_agent(),
        timestamp,
        message,
        build_term_selector(selector),
        target_data[ODS_ID],
        target_data[ODS_TYPE],
        dcterms_ref,
        "oa:commenting",
    )


def map_to_annotation_str_val(
    ods_agent: Dict,
    timestamp: str,
    oa_value: str,
    oa_selector: Dict,
    target_id: str,
    target_type: str,
    dcterms_ref: str,
    motivation: str,
) -> Dict[str, Any]:
    """
     Map the result of the API call to an annotation. Uses a string value
    :param ods_agent: Agent object of MAS
    :param timestamp: A formatted timestamp of the current time
    :param oa_value: Value of the body of the annotation, the result of the computation
    :param oa_selector: selector of this annotation
    :param target_id: ID of target maps to dcterms:identifier
    :param target_type: target Type, maps to ods:type
    :param dcterms_ref: maps to dcterms:references
    :param motivation: motivation of the annotation
    :return: Returns a formatted annotation Record
    """
    return {
        AT_TYPE: "ods:Annotation",
        "oa:motivation": motivation,
        "dcterms:creator": ods_agent,
        "dcterms:created": timestamp,
        "oa:hasTarget": {
            ODS_ID: target_id,
            AT_ID: target_id,
            ODS_TYPE: target_type,
            AT_TYPE: target_type,
            "oa:hasSelector": oa_selector,
        },
        "oa:hasBody": {
            AT_TYPE: "oa:TextualBody",
            "oa:value": [oa_value],
            "dcterms:references": dcterms_ref,
        },
    }


def map_to_annotation(
    ods_agent: Dict,
    timestamp: str,
    oa_value: Dict,
    oa_selector: Dict,
    target_id: str,
    target_type: str,
    dcterms_ref: str,
) -> Dict[str, Any]:
    """
    Map the result of the API call to an annotation
    :param ods_agent: Agent object of MAS
    :param timestamp: A formatted timestamp of the current time
    :param oa_value: Value of the body of the annotation, the result of the computation
    :param oa_selector: selector of this annotation
    :param target_id: ID of target maps to dcterms:identifier
    :param target_type: target Type, maps to ods:type
    :param dcterms_ref: maps tp dcterms:references
    :return: Returns a formatted annotation Record
    """
    return map_to_annotation_str_val(
        ods_agent,
        timestamp,
        json.dumps(oa_value),
        oa_selector,
        target_id,
        target_type,
        dcterms_ref,
        "ods:adding",
    )


def build_class_selector(oa_class: str) -> Dict:
    """
    Builds Selector for annotations that affect classes
    :param oa_class: The full jsonPath of the class being annotate
    :return: class selector object
    """
    return {
        AT_TYPE: "ods:ClassSelector",
        "ods:class": oa_class,
    }


def build_term_selector(ods_term: str) -> Dict:
    """
    A selector for an individual field.
    :param ods_term: The full jsonPath of the field being annotated
    :return: field selector object
    """
    return {AT_TYPE: "ods:TermSelector", "ods:term": ods_term}


def build_fragment_selector(bounding_box: Dict, width: int, height: int) -> Dict:
    """
    A selector for a specific Region of Interest (Roi). Only applicable on media objects.
    Conforms to the TDWG region of interest vocabulary.
    :param bounding_box: object containing the bounding box of the ROI
    :param width: The width of the image, used to calculate the ROI
    :param height: the height of the image, used to calculate the ROI
    :return: A fragment selector for a specific ROI
    """
    return {
        AT_TYPE: "oa:FragmentSelector",
        "dcterms:conformsTo": "https://ac.tdwg.org/termlist/#711-region-of-interest-vocabulary",
        "ac:hasROI": {
            "ac:xFrac": bounding_box["boundingBox"][0] / width,
            "ac:yFrac": bounding_box["boundingBox"][1] / height,
            "ac:widthFrac": (bounding_box["boundingBox"][2] - bounding_box["boundingBox"][0]) / width,
            "ac:heightFrac": (bounding_box["boundingBox"][3] - bounding_box["boundingBox"][1]) / height,
        },
    }


def build_entire_image_fragment_selector(width: int, height: int) -> Dict:
    """
    Build a selector for the entire object. Only applicable on media objects.
    Conforms to the TDWG region of interest vocabulary.
    :param width: The width of the image
    :param height: the height of the image
    :return: A fragment selector for the entire image
    """
    return {
        AT_TYPE: "oa:FragmentSelector",
        "dcterms:conformsTo": "https://ac.tdwg.org/termlist/#711-region-of-interest-vocabulary",
        "ac:hasROI": {
            "ac:xFrac": 0,
            "ac:yFrac": 0,
            "ac:widthFrac": width,
            "ac:heightFrac": height,
        },
    }


def send_failed_message(job_id: str, message: str, channel: BlockingChannel) -> None:
    """
    Sends a failure message to the mas failure topic, mas-failed
    :param job_id: The id of the job
    :param message: The exception message
    :param channel: A RabbitMQ BlockingChannel to which we will publish the failure message
    """
    logging.error(f"Job {job_id} failed with error: {message}")
    mas_failed = {"jobId": job_id, "errorMessage": message}
    channel.basic_publish(
        exchange=os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-failed-exchange"),
        routing_key=os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation-failed"),
        body=json.dumps(mas_failed).encode("utf-8"),
    )
