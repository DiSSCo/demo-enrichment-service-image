import json
import logging
import os
import uuid
from typing import Dict

import requests
from kafka import KafkaConsumer, KafkaProducer
import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


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
        try:
            logging.info("Received message: " + str(msg.value))
            json_value = msg.value
            shared.mark_job_as_running(json_value.get("jobId"))
            specimen_data = json_value.get("object")
            result = run_api_call(specimen_data)
            annotation_event = map_to_annotation_event(
                specimen_data, result, json_value.get("jobId")
            )
            publish_annotation_event(annotation_event, producer)
        except Exception as e:
            logging.exception(e)


def map_to_annotation_event(
    specimen_data: Dict, result: Dict[str, str], job_id: str
) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result which contains either the GBIF ID or an error message
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if result.get("error_message") is not None:
        return {
            "jobId": job_id,
            "annotations": [
                shared.map_to_empty_annotation(
                    timestamp,
                    result.get("error_message"),
                    specimen_data,
                    shared.ER_PATH,
                    result.get("queryString"),
                )
            ],
        }
    ods_agent = shared.get_agent()
    oa_value = shared.map_to_entity_relationship(
        "hasGbifID",
        result.get("gbifID"),
        f'https://www.gbif.org/occurrence/{result.get("gbifID")}',
        timestamp,
        ods_agent,
    )
    oa_selector = shared.build_class_selector(shared.ER_PATH)
    annotation = shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )

    return {"jobId": job_id, "annotations": [annotation]}


def publish_annotation_event(annotation: Dict, producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info("Publishing annotation: " + str(annotation))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation)


def run_api_call(specimen_data: Dict) -> Dict[str, str]:
    """
    Calls GBIF API based on the occurrenceID, catalogNumber and basisOfRecord
    :param specimen_data: The JSON data of the Digital Specimen
    :return: The result from the API, which contains either the GBIF ID or an error message
    """
    identifiers = get_identifiers_from_object(specimen_data)
    query_string = (
        f"https://api.gbif.org/v1/occurrence/search?occurrenceID="
        f"{identifiers.get('occurrenceID')}"
        f"&basisOfRecord={specimen_data.get('dwc:basisOfRecord')}"
    )
    if specimen_data.get("catalogNumber") is not None:
        query_string = (
            query_string + f"&catalogNumber={identifiers.get('catalogNumber')}"
        )
    response = requests.get(query_string)
    response_json = json.loads(response.content)
    if response_json.get("count") == 1:
        logging.info(
            "Successfully retrieved a single result from GBIF based on the identifiers"
        )
        return {
            "queryString": query_string,
            "gbifID": response_json.get("results")[0].get("gbifID"),
        }
    elif response_json["count"] == 0:
        logging.info("No results were returned, unable to create a relationship")
        return {
            "queryString": query_string,
            "error_message": "Failed to make the match, no match could be created",
        }
    else:
        logging.info("More than one result returned, unable to create a relationship")
        return {
            "queryString": query_string,
            "error_message": "Failed to make the match, too many candidates",
        }


def get_identifiers_from_object(specimen_data: Dict) -> Dict[str, str]:
    """
    Retrieve the correct identifiers from the Digital Specimen
    :param specimen_data: Json data of the Digital Specimen
    :return: The mapped relevant_identifiers (occurrenceID and catalogNumber)
    """
    relevant_identifiers = {}
    for identifier in specimen_data.get("ods:hasIdentifiers"):
        if identifier.get("dcterms:title") in ["dwc:occurrenceID", "abcd:unitGUID"]:
            relevant_identifiers["occurrenceID"] = identifier.get("dcterms:identifier")
        if identifier.get("dcterms:title") in ["dwc:catalogNumber", "abcd:unitID"]:
            relevant_identifiers["catalogNumber"] = identifier.get("dcterms:identifier")
    return relevant_identifiers


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
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
    start_kafka()
    #run_local("https://sandbox.dissco.tech/api/digital-specimen/v1/SANDBOX/A7D-9PL-3YP")
