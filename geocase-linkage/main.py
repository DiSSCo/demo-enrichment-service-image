import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List

import requests
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

ODS_TYPE = "ods:type"
ODS_ID = "ods:id"


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'),
                             group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        try:
            logging.info('Received message: ' + str(msg.value))
            json_value = msg.value
            specimen_data = json_value['object']['digitalSpecimen']
            result = run_api_call(specimen_data)
            mas_job_record = map_to_mas_job_record(specimen_data, result, json_value["jobId"])
            send_updated_opends(mas_job_record, producer)
        except Exception as e:
            logging.exception(e)


def map_to_mas_job_record(specimen_data: Dict, results: List[Dict[str, str]], job_id: str) -> dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = timestamp_now()
    if results is None:
        annotations = list()
    else:
        annotations = list(map(lambda result: map_to_annotation(specimen_data, result, timestamp), results))
    mas_job_record = {
        "jobId": job_id,
        "annotations": annotations
    }
    return mas_job_record


def map_to_annotation(specimen_data, result, timestamp):
    oa_value = {
        'entityRelationships': {
            'entityRelationshipType': 'hasGeoCASeID',
            'objectEntityIri': f'https://geocase.eu/specimen/{result["geocaseId"]}',
            'entityRelationshipDate': timestamp,
            'entityRelationshipCreatorName': os.environ.get('MAS_NAME'),
            'entityRelationshipCreatorId': f"https://hdl.handle.net/{os.environ.get('MAS_ID')}"
        }
    }
    annotation = {
        'rdf:type': 'Annotation',
        'oa:motivation': 'ods:adding',
        'oa:creator': {
            ODS_TYPE: 'oa:SoftwareAgent',
            'foaf:name': os.environ.get('MAS_NAME'),
            ODS_ID: f"https://hdl.handle.net/{os.environ.get('MAS_ID')}"
        },
        'dcterms:created': timestamp,
        'oa:target': {
            ODS_ID: specimen_data[ODS_ID],
            ODS_TYPE: specimen_data[ODS_TYPE],
            'oa:selector': {
                ODS_TYPE: 'ClassSelector',
                'oa:class': '$.entityRelationships'
            },
        },
        'oa:body': {
            ODS_TYPE: 'TextualBody',
            'oa:value': [json.dumps(oa_value)],
            'dcterms:reference': result['queryString']
        }
    }
    return annotation


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
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation)


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
        hits = response_json.get('response').get('numFound')
        if hits <= 5:
            return list(map(
                lambda result: {'queryString': query_string, 'geocaseId': result['geocase_id']},
                response_json.get('response').get('docs')))
        else:
            logging.info(f'Too many hits ({hits}) were found for specimen: {specimen_data["ods:id"]}')
    else:
        logging.info(f'No relevant identifiers found for specimen: {specimen_data["ods:id"]}')


def build_query_string(identifiers: Dict[str, str]):
    """
    Build query string from all identifiers in the Digital Specimen
    :param identifiers: All identifiers in the digital specimen
    :return: A formatted query string
    """
    query_string = 'https://api.geocase.eu/v1/solr?q='
    for key, value in identifiers.items():
        if not query_string.endswith('q='):
            query_string = query_string + ' AND '
        query_string = query_string + f'{key}:"{value}"'
    return query_string


def get_identifiers_from_object(specimen_data: Dict) -> Dict[str, str]:
    """
    Retrieve the correct identifiers from the Digital Specimen
    :param specimen_data: Json data of the Digital Specimen
    :return: The mapped relevant_identifiers (unitId and recordURI)
    """
    relevant_identifiers = {}
    for identifier in specimen_data['identifiers']:
        if identifier.get('???:identifierType') in ['abcd:unitID']:
            relevant_identifiers['unitid'] = identifier.get('???:identifierValue')
        if identifier.get('???:identifierType') in ['abcd:recordURI']:
            relevant_identifiers['recordURI'] = identifier.get('???:identifierValue')
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
    attributes = json.loads(response.content)['data']['attributes']
    specimen_data = attributes['digitalSpecimen']
    result = run_api_call(specimen_data)
    mas_job_record = map_to_mas_job_record(specimen_data, result, str(uuid.uuid4()))
    logging.info('Created annotations: ' + str(mas_job_record))


if __name__ == '__main__':
    start_kafka()
