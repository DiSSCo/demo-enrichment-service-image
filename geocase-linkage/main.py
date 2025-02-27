import json
import logging
import os
import uuid
from typing import Dict, List

import requests
from kafka import KafkaConsumer, KafkaProducer
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


def map_to_annotation_event(specimen_data: Dict, results: List[Dict[str, str]], job_id: str) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if len(results) == 1 and results[0].get("errors") is not None:
        annotations = [
            shared.map_to_empty_annotation(
                timestamp, results[0].get("errors"), specimen_data,
                shared.ER_PATH, results[0].get("queryString"))
        ]
    else:
        ods_agent = shared.get_agent()
        annotations = list(
            map(lambda result: map_result_to_annotation(specimen_data, result, timestamp, ods_agent), results)
        )
    return {"jobId": job_id, "annotations": annotations}


def map_result_to_annotation(specimen_data: Dict, result: Dict, timestamp: str, ods_agent: Dict) -> Dict:
    """
    maps result of geocase linkage to an annotation
    :param specimen_data: specimen data
    :param result: result of the linkage search
    :param timestamp: formatted timestamp
    :param ods_agent: agent of this mas
    :return:
    """
    oa_value = shared.map_to_entity_relationship(
        "hasGeoCASeID", result["geocaseId"], f'https://geocase.eu/specimen/{result["geocaseId"]}', timestamp, ods_agent
    )
    oa_selector = shared.build_class_selector(shared.ER_PATH)
    return shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )


def publish_annotation_event(annotation: Dict, producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info("Publishing annotation: " + str(annotation))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation)


def run_api_call(specimen_data: Dict) -> List[Dict[str, str]]:
    """
    Calls GeoCASe API based on the available identifiers, unitId and/or recordURI.
    It is possible that one Digital Specimen has multiple GeoCASe records.
    If we get more than 5 GeoCASe hits we assume that something went wrong and we will not return any results.
    :param specimen_data: The JSON data of the Digital Specimen
    :return:  A list of results that contain the queryString and the geoCASe identifier
    """
    identifiers = get_identifiers_from_object(specimen_data)
    if identifiers and len(identifiers) > 0:
        query_string = build_query_string(identifiers)
        response = requests.get(query_string)
        response_json = json.loads(response.content)
        hits = response_json.get("response").get("numFound")
        if hits <= 5:
            return list(
                map(
                    lambda result: {"queryString": query_string, "geocaseId": result["geocase_id"]},
                    response_json.get("response").get("docs"),
                )
            )
        else:
            logging.info(f"Too many hits ({hits}) were found for specimen: {specimen_data[shared.ODS_ID]}")
            return [
                {
                    "queryString": query_string, "geocaseId": None,
                    "errors": "Failed to make the match, too many candidates"
                }
            ]
    else:
        logging.info(f"No relevant identifiers found for specimen: {specimen_data[shared.ODS_ID]}")
        return [
            {
                "queryString": "", "geocaseId": None,
                "errors": "Failed to make the match, No relevant identifiers found for specimen"
            }
        ]


def build_query_string(identifiers: Dict[str, str]):
    """
    Build query string from all identifiers in the Digital Specimen
    :param identifiers: All identifiers in the digital specimen
    :return: A formatted query string
    """
    query_string = "https://api.geocase.eu/v1/solr?q="
    for key, value in identifiers.items():
        if not query_string.endswith("q="):
            query_string = query_string + " AND "
        query_string = query_string + f'{key}:"{value}"'
    return query_string


def get_identifiers_from_object(specimen_data: Dict) -> Dict[str, str]:
    """
    Retrieve the correct identifiers from the Digital Specimen
    :param specimen_data: Json data of the Digital Specimen
    :return: The mapped relevant_identifiers (unitId and recordURI)
    """
    relevant_identifiers = {}
    for identifier in specimen_data["ods:hasIdentifiers"]:
        if identifier.get("dcterms:title") in ["abcd:unitID"]:
            relevant_identifiers["unitid"] = identifier.get("dcterms:identifier")
        if identifier.get("dcterms:title") in ["abcd:recordURI"]:
            relevant_identifiers["recordURI"] = identifier.get("dcterms:identifier")
    return relevant_identifiers


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/ZWL-YMS-4WY
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen_data = json.loads(response.content).get("data").get("attributes")
    result = run_api_call(specimen_data)
    mas_job_record = map_to_annotation_event(specimen_data, result, str(uuid.uuid4()))
    logging.info("Created annotations: " + str(mas_job_record))


if __name__ == "__main__":
    start_kafka()
    # run_local('https://dev.dissco.tech/api/digital-specimen/v1/TEST/SGT-C68-7KY')
