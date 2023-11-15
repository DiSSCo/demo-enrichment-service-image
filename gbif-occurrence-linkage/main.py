import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict

import requests
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

ODS_TYPE = "ods:type"
ODS_ID = "ods:id"


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'),
                             group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        logging.info('Received message: ' + str(msg.value))
        json_value = msg.value
        specimen_data = json_value['object']['digitalSpecimen']
        result = run_api_call(specimen_data)
        if result.get('gbif_id') is not None:
            annotations = map_to_annotation(specimen_data, result, json_value["jobId"])
            send_updated_opends(annotations, producer)


def map_to_annotation(specimen_data: Dict, result: Dict[str, str], job_id: str) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result which contains either the GBIF ID or an error message
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = timestamp_now()
    annotation = {
        'rdf:type': 'Annotation',
        'oa:motivation': 'ods:adding',
        'oa:creator': {
            ODS_TYPE: 'machine',
            'foaf:name': os.environ.get('MAS_NAME'),
            ODS_ID: os.environ.get('MAS_ID')
        },
        'dcterms:created': timestamp,
        'oa:target': {
            ODS_ID: specimen_data[ODS_ID],
            ODS_TYPE: specimen_data[ODS_TYPE],
            'oa:selector': {
                ODS_TYPE: 'ClassSelector',
                'oa:class': 'entityRelationship'
            },
        },
        'oa:body': {
            ODS_TYPE: 'TextualBody/Other',
            'oa:value': [{
                'entityRelationship': {
                    'entityRelationshipType': 'hasGbifID',
                    'objectEntityIri': f'https://www.gbif.org/occurrence/{result["gbifId"]}',
                    'entityRelationshipDate': timestamp,
                    'entityRelationshipCreatorName': 'GBIF occurrence linker',
                    'entityRelationshipCreatorId': 'https://hdl.handle.net/enrichment-service-pid'
                }
            }],
            'dcterms:reference': result['queryString']
        }
    }
    mas_job_record = {
        "jobId": job_id,
        "annotations": [annotation]
    }
    return mas_job_record


def timestamp_now() -> str:
    """
    Create a timestamp in the correct format
    :return: The timestamp as a string
    """
    timestamp = str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone


def send_updated_opends(annotation: Dict, producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info('Publishing annotation: ' + str(annotation))
    producer.send('annotation', annotation)


def run_api_call(specimen_data: Dict) -> Dict[str, str]:
    """
    Calls GBIF API based on the occurrenceID, catalogNumber and basisOfRecord
    :param specimen_data: The JSON data of the Digital Specimen
    :return: The result from the API, which contains either the GBIF ID or an error message
    """
    identifiers = get_identifiers_from_object(specimen_data)
    query_string = (f'https://api.gbif.org/v1/occurrence/search?occurrenceID='
                    f'{identifiers.get("occurrenceId")}&catalogNumber={identifiers.get("catalogNumber")}'
                    f'&basisOfRecord={specimen_data["dwc:basisOfRecord"]}')
    response = requests.get(query_string)
    response_json = json.loads(response.content)
    if response_json['count'] == 1:
        logging.info('Successfully retrieved a single result from GBIF based on the identifiers')
        return {'queryString': query_string, 'gbifId': response_json['results'][0]['gbifID']}
    elif response_json['count'] == 0:
        logging.info('No results were returned, unable to create a relationship')
        return {'queryString': query_string, 'error_message': 'Failed to make the match, no match could be created'}
    else:
        logging.info('More than one results returned, unable to create a relationship')
        return {'queryString': query_string, 'error_message': 'Failed to make the match, too many candidates'}


def get_identifiers_from_object(specimen_data: Dict) -> Dict[str, str]:
    """
    Retrieve the correct identifiers from the Digital Specimen
    :param specimen_data: Json data of the Digital Specimen
    :return: The mapped relevant_identifiers (occurrenceId and catalogNumber)
    """
    relevant_identifiers = {}
    for identifier in specimen_data['identifiers']:
        if identifier.get('???:identifierType') in ['dwc:occurrenceID', 'abcd:unitGUID']:
            relevant_identifiers['occurrenceId'] = identifier.get('???:identifierValue')
        if identifier.get('???:identifierType') in ['dwc:catalogNumber', 'abcd:unitID']:
            relevant_identifiers['catalogNumber'] = identifier.get('???:identifierValue')
    return relevant_identifiers


def run_local(example: str) -> None:
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/M88-SJK-MDP
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    attributes = json.loads(response.content)['data']['attributes']
    specimen_data = attributes['digitalSpecimen']
    result = run_api_call(specimen_data)
    annotations = map_to_annotation(specimen_data, result, str(uuid.uuid4()))
    logging.info('Created annotations: ' + str(annotations))


if __name__ == '__main__':
    start_kafka()
