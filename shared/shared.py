from datetime import datetime, timezone
from typing import Dict, Any
import json
import os
import requests

ODS_TYPE = "ods:type"
AT_TYPE = "@type"
ODS_ID = "ods:ID"
AT_ID = "@id"
MAS_ID = os.environ.get('MAS_ID')
MAS_NAME = os.environ.get('MAS_NAME')

def timestamp_now() -> str:
    """
    Create a timestamp in the correct format
    :return: The timestamp as a string
    """
    timestamp = str(
        datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone


def mark_job_as_running(job_id: str):
    """
    Calls DiSSCo's RUNNING endpoint to inform the system that the message has
    been received by the MAS. Doing so will update the status of the job to
    "RUNNING" for any observing users.
    :param job_id: the job id from the kafka message
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
        AT_TYPE: 'as:Application',
        'schema:name': MAS_NAME,
        'ods:hasIdentifier': [
            {
                AT_TYPE: "ods:Identifier",
                'dcterms:title': 'handle',
                'dcterms:identifier': f"https://hdl.handle.net/{MAS_ID}"
            }
        ]
    }


def map_to_entity_relationship(relationship_type: str, resource_id: str,
                               timestamp: str, ods_agent: Dict) -> Dict:
    """
    :param relationship_type: Maps to dwc:relationshipOfResource
    :param resource_id: Id of related resource, maps to dwc:relatedResourceID and ods:relatedResourceURI
    :param timestamp: timestamp of ER creation
    :param ods_agent: MAS as agent object
    :return: formatted Entity relationship annotation
    """
    return {
        AT_TYPE: 'ods:EntityRelationship',
        'dwc:relationshipOfResource': relationship_type,
        'dwc:relatedResourceID': resource_id,
        'ods:relatedResourceURI': resource_id,
        'dwc:relationshipEstablishedDate': timestamp,
        'ods:RelationshipAccordingToAgent': ods_agent
    }


def map_to_annotation(ods_agent: Dict, timestamp: str,
                      oa_value: Dict, oa_selector: Dict, target_id: str, target_type: str,
                      dcterms_ref: str) -> Dict[str, Any]:
    """
    Map the result of the API call to an annotation
    :param ods_agent: Agent object of MAS
    :param timestamp: A formatted timestamp of the current time
    :param oa_value: Value of the body of the annotation, the result of the computation
    :param oa_selector: selector of this annotation
    :param target_id: ID of target maps to ods:ID
    :param target_type: target Type, maps to ods:type
    :param dcterms_ref: maps tp dcterms:references
    :return: Returns a formatted annotation Record
    """
    annotation = {
        AT_TYPE: 'ods:Annotation',
        'oa:motivation': 'ods:adding',
        'dcterms:creator': ods_agent,
        'dcterms:created': timestamp,
        'oa:hasTarget': {
            ODS_ID: target_id,
            AT_ID: target_id,
            ODS_TYPE: target_type,
            AT_TYPE: target_type,
            'oa:hasSelector': oa_selector,
        },
        'oa:hasBody': {
            AT_TYPE: 'oa:TextualBody',
            'oa:value': [json.dumps(oa_value)],
            'dcterms:references': dcterms_ref,
        },
    }
    return annotation


def build_class_selector(oa_class: str) -> Dict:
    """
    Builds Selector for annotations that affect classes
    :param oa_class: The full jsonPath of the class being annotate
    :return: class selector object
    """
    return {
        AT_TYPE: 'ods:ClassSelector',
        'ods:class': oa_class,
    }


def build_field_selector(ods_field: str) -> Dict:
    """
    A selector for an individual field.
    :param ods_field: The full jsonPath of the field being annotated
    :return: field selector object
    """
    return {
        AT_TYPE: 'ods:FieldSelector',
        'ods:field': ods_field
    }


def build_fragment_selector(bounding_box: Dict, width: int,
                            height: int) -> Dict:
    """
    A selector for a specific Region of Interest (Roi). Only applicable on media objects
    :param bounding_box: object containing the bounding box of the ROI
    :param width: The width of the image, used to calculate the ROI
    :param height: the height of the image, used to calculate the ROI
    :return:
    """
    return {
        ODS_TYPE: 'oa:FragmentSelector',
        'dcterms:conformsTo': 'https://www.w3.org/TR/media-frags/',
        'ac:hasROI': {
            'ac:xFrac': bounding_box['boundingBox'][0] / width,
            'ac:yFrac': bounding_box['boundingBox'][1] / height,
            'ac:widthFrac': (bounding_box['boundingBox'][2] -
                             bounding_box['boundingBox'][0]) / width,
            'ac:heightFrac': (bounding_box['boundingBox'][3]
                              - bounding_box['boundingBox'][1]) / height
        }
    }
