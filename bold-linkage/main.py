import json
import logging
import os
import uuid
from typing import Dict, List

import requests
from kafka import KafkaConsumer, KafkaProducer
from requests.auth import HTTPBasicAuth
import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service
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
        try:
            logging.info("Received message: " + str(msg.value))
            json_value = msg.value
            shared.mark_job_as_running(json_value.get("jobId"))
            specimen_data = json_value.get("object")
            result = run_api_call(specimen_data)
            mas_job_record = map_to_annotation_event(specimen_data, result, json_value.get("jobId"))
            publish_annotation_event(mas_job_record, producer)
        except Exception as e:
            logging.exception(e)


def map_to_annotation_event(specimen_data: Dict, results: List[Dict[str, str]], job_id: str) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the BOLD EU process identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if results is None:
        annotations = list()
    else:
        annotations = list(
            map(
                lambda result: map_result_to_annotation(specimen_data, result, timestamp),
                results,
            )
        )
    annotation_event = {"jobId": job_id, "annotations": annotations}
    return annotation_event


def map_result_to_annotation(specimen_data: Dict, result: Dict[str, str], timestamp: str) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The original specimen data
    :param result: The result from BOLD EU, contains the Bold EU processid and the queryString
    :param timestamp: A formatted timestamp of the current time
    :return: Returns a formatted annotation
    """
    ods_agent = shared.get_agent()
    oa_value = shared.map_to_entity_relationship(
        "hasBOLDEUProcessID",
        result["processid"],
        f"https://boldsystems.eu/record/{result['processid']}",
        timestamp,
        ods_agent,
    )
    oa_selector = shared.build_class_selector("$['ods:hasEntityRelationships']")
    return shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )


def publish_annotation_event(annotation_event: Dict, producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info("Publishing annotation: " + str(annotation_event))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)


def run_api_call(specimen_data: Dict) -> List[Dict[str, str]]:
    """
    Calls BOLD EU API based on the available identifiers.
    It is possible that one Digital Specimen has multiple BOLD records.
    :param specimen_data: The JSON data of the Digital Specimen
    :return:  A list of results that contain the queryString and the BOLD EU process ids
    """
    # The endpoint for queries
    query_endpoint = "https://boldsystems.eu/api/query"

    # Get one or more specimen IDs. Considering that this will be a GET request this can't
    # be enormous - but perhaps 100 IDs is fine as a batch size.
    identifiers = list(
        map(
            lambda identifier: identifier.get("dcterms:identifier"),
            specimen_data.get("ods:hasIdentifiers"),
        )
    )
    # BOLD's API has a concept of 'scope' (here: 'ids') and 'subscope' (here: 'sampleid'),
    # where the value is the third part of a triple. All triples are joined with commas.
    query_value = ",".join([f"ids:sampleid:{id}" for id in identifiers])

    # Compose the URL and define the accept header
    headers = {"Accept": "application/json"}
    query_string = f"{query_endpoint}?query={query_value}&extent=full"

    # Do the request
    response = requests.get(
        query_string,
        headers=headers,
        auth=HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD")),
    )
    response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx

    # The response is a token from which a URL needs to be composed whose dereferencing
    # results in a set of records
    query_id = response.json()["query_id"]

    # Compose the URL for getting results. It is possible to page through results, so
    # you can specify how many you want per page (`length`) and the zero-based offset
    # assuming no more than 25 results per specimen are returned
    docs_endpoint = f"https://boldsystems.eu/api/documents/{query_id}?length=25&start=0"

    # Fetch the documents
    response = requests.get(
        docs_endpoint,
        headers=headers,
        auth=HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD")),
    )
    response.raise_for_status()  # Ensure the request was successful

    # Parse the response to get the records
    records = response.json()["data"]

    return list(
        map(
            lambda record: {
                "queryString": query_string,
                "processid": record["processid"],
            },
            records,
        )
    )


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/S0P-JMS-V4Q
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen = json.loads(response.content).get("data")
    specimen_data = specimen.get("attributes")
    result = run_api_call(specimen_data)
    mas_job_record = map_to_annotation_event(specimen_data, result, str(uuid.uuid4()))
    logging.info("Created annotations: " + json.dumps(mas_job_record, indent=2))


if __name__ == "__main__":
    start_kafka()
    # run_local("https://sandbox.dissco.tech/api/v1/specimens/SANDBOX/NMT-F9R-FWK")
