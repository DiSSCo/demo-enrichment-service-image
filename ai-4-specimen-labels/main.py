import json
import logging
import os
from typing import Dict, Any, List, Tuple
from requests.auth import HTTPBasicAuth

import requests
from kafka import KafkaConsumer, KafkaProducer
import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
OA_BODY = "oa:hasBody"
DWC_LOCALITY = "dwc:locality"
ODS_HAS_EVENTS = "ods:hasEvents"
ODS_HAS_LOCATION = "ods:hasLocation"
OA_VALUE = "oa:value"
LOCATION_PATH = f"$['{ODS_HAS_EVENTS}'][*]['{ODS_HAS_LOCATION}']"
USER_AGENT = "Distributed System of Scientific Collections"

"""
This is a template for a simple MAS that calls an API and sends a list of annotations back to DiSSCo. 
"""


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    """
    consumer = KafkaConsumer(
        os.environ.get("KAFKA_CONSUMER_TOPIC"),
        group_id=os.environ.get("KAFKA_CONSUMER_GROUP"),
        bootstrap_servers=[os.environ.get("KAFKA_CONSUMER_HOST")],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_PRODUCER_HOST")],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    for msg in consumer:
        logging.info(f"Received message: {str(msg.value)}")
        json_value = msg.value
        # Indicates to DiSSCo the message has been received by the mas and the job is running.
        # DiSSCo then informs the user of this development
        shared.mark_job_as_running(job_id=json_value.get("jobId"))
        digital_object = json_value.get("object")
        try:
            annotations = build_annotations(digital_object)
            event = {"annotations": annotations, "jobId": json_value.get("jobId")}
            logging.info(f"Publishing annotation event: {json.dumps(event)}")
            publish_annotation_event(event, producer)
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")
            send_failed_message(json_value.get("jobId"), str(e), producer)


def build_query_string(digital_object: Dict[str, Any]) -> Tuple[str, List[str]]:
    """
    Builds the query for n8n endpoint
    :param digital_object: Target of the annotation
    :return: query string to some example API, list of access URIs
    """
    access_uris = [digital_object.get("ac:accessURI")]
    # Use your API here
    return f"https://n8n.svc.gbif.no/webhook/9fa39dd6-63ea-4ed8-b4e1-904051e8a41a", access_uris


def publish_annotation_event(
        annotation_event: Dict[str, Any], producer: KafkaProducer
) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted list of annotations
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info(f"Publishing annotation: {str(annotation_event)}")
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)


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
        return [shared.map_to_empty_annotation(
            timestamp,
            "Unable to read specimen label",
            digital_media[shared.ODS_ID],
            digital_media[shared.ODS_TYPE],
            query_string
        )]
    specimen = get_specimen(digital_media)
    taxon_identification, json_path = get_taxon_identification(specimen)
    annotations = list()

    for field in response['data']:
        if field in taxon_identification.keys():
            unchanged = response['data'][field] == taxon_identification[field]
            if unchanged:
                logging.debug(f"No new information for {field}")
                annotations.append(shared.map_to_annotation_str_val(
                    shared.get_agent(),
                    timestamp,
                    "Existing information aligns with AI reading",
                    shared.build_term_selector(json_path),
                    specimen[shared.ODS_ID],
                    specimen[shared.ODS_TYPE],
                    f"query_string&{response['version']}",
                    "oa:assessing"
                ))
            else:
                logging.info("change!")

    # todo for field in value make annotation
    return annotations


def get_specimen(digital_media: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes media object and returns related specimen (max 1)
    :param digital_media: Media object to get related specimen
    """
    entity_relationships = digital_media.get('ods:hasEntityRelationships')
    for entity_relationship in entity_relationships:
        if entity_relationship.get('dwc:relationshipOfResource') == "hasDigitalSpecimen":
            specimen_doi = entity_relationship.get('dwc:relatedResourceID').replace("https://doi.org/", "")
            break
    return json.loads(requests.get(f"{os.environ.get('DISSCO_API_SPECIMEN')}/{specimen_doi}").content).get('data').get('attributes')


def get_taxon_identification(digital_specimen: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    for id_idx, identification in enumerate(digital_specimen.get("ods:hasIdentifications")):
        for tax_idx, taxonIdentification in enumerate(identification.get("ods:hasTaxonIdentifications")):
            if taxonIdentification.get("dwc:taxonomicStatus") == "ACCEPTED":
                return taxonIdentification, f"$['ods:hasIdentifications'][{id_idx}]['ods:hasTaxonIdentifications'][{tax_idx}]"
    return {}, ""



def run_api_call(query_string: str, uris: List[str]) -> Dict[str, Any]:
    try:
        auth = HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD"))
        response = requests.post(query_string, json={
            "uris": uris}, auth=auth)
        response.raise_for_status()
        response_json = json.loads(response.content)
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException
    '''
    It is up to the MAS developer to determine the best format for the value of the annotation.
    '''
    return response_json


def send_failed_message(job_id: str, message: str, producer: KafkaProducer) -> None:
    """
    Sends a failure message to the mas failure topic, mas-failed
    :param job_id: The id of the job
    :param message: The exception message
    :param producer: The Kafka producer
    """

    mas_failed = {
        "jobId": job_id,
        "errorMessage": message
    }
    producer.send("mas-failed", mas_failed)


def run_local(media_id: str):
    """
    Runs script locally. Demonstrates using a specimen target
    :param media_id: A media ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_media = (
        requests.get(
            f"https://sandbox.dissco.tech/api/digital-media/v1/{media_id}"
        )
        .json()
        .get("data")
        .get("attributes")
    )

    specimen_annotations = build_annotations(digital_media)
    event = {specimen_annotations, "Some job ID"}
    logging.info(f"created annotation event: {json.dumps(event)}")


if __name__ == '__main__':
    # start_kafka()
    run_local("SANDBOX/LFE-4MF-LCD")