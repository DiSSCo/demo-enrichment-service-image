import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any

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
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'), group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        logging.info('Received message: ' + str(msg.value))
        json_value = msg.value
        specimen_data = json_value['object']['digitalSpecimen']
        result = run_georeference(specimen_data)
        mas_job_record = map_to_mas_job_record(specimen_data, result, str(uuid.uuid4()))
        send_updated_opends(mas_job_record, producer)


def map_to_mas_job_record(specimen_data: Dict, results: List[Dict[str, str]], job_id: str) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = timestamp_now()
    annotations = list(map(lambda result: map_to_georeference_annotation(specimen_data, result, timestamp), results))
    annotations.append(
        list(map(lambda result: map_to_entity_relationship_annotation(specimen_data, result, timestamp), results)))
    mas_job_record = {
        "jobId": job_id,
        "annotations": annotations
    }
    return mas_job_record


def map_to_entity_relationship_annotation(specimen_data: Dict, result: Dict[str, Any], timestamp: str):
    """
    Map the result of the Mindat Locality API call to an entityRelationship annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Mindat Locality API call
    :param timestamp: The current timestamp
    :return:  A single annotation with the relationship to the Mindat locality
    """
    oa_value = {
        'entityRelationships': {
            'entityRelationshipType': 'hasMindatLocation',
            'objectEntityIri': f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
            'entityRelationshipDate': timestamp,
            'entityRelationshipCreatorName': os.environ.get('MAS_NAME'),
            'entityRelationshipCreatorId': f"https://hdl.handle.net/{os.environ.get('MAS_ID')}"
        }
    }
    return wrap_oa_value(oa_value, result, specimen_data, timestamp, '$./entityRelationships')


def map_to_georeference_annotation(specimen_data: Dict, result: Dict[str, Any], timestamp: str):
    """
    Map the result of the Mindat Locality API call to a georeference annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Mindat Locality API call
    :param timestamp: The current timestamp
    :return: A single annotation with the georeference information from the Mindat locality
    """
    oa_value = {
        "georeference": {
            "dwc:decimalLatitude": result['geo_reference_result']['latitude'],
            "dwc:decimalLongitude": result['geo_reference_result']['longitude'],
            "dwc:geodeticDatum": 'WGS84',
            "dwc:georeferencedBy": f"https://hdl.handle.net/{os.environ.get('MAS_ID')}",
            "dwc:georeferencedDate": timestamp,
            "dwc:georeferenceSources": f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
            "dwc:georeferenceProtocol": "Georeferenced against the Mindat Locality API based on the specimen "
                                        "locality string (dwc:locality)"
        }
    }

    return wrap_oa_value(oa_value, result, specimen_data, timestamp,
                         f"$./occurrences[{result['occurrence_index']}].location.georeference")


def wrap_oa_value(oa_value: Dict, result: Dict[str, Any], specimen_data: Dict, timestamp: str, oa_class: str) -> Dict:
    """
    Generic method to wrap the oa_value into an annotation object
    :param oa_value: The value that contains the result of the MAS
    :param result: The result of the Mindat Locality API call
    :param specimen_data: The JSON value of the Digital Specimen
    :param timestamp: The current timestamp
    :param oa_class: The name of the class to which the class annotation points
    :return: Returns an annotation with all the relevant metadata
    """
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
                'oa:class': oa_class
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


def run_georeference(specimen_data: Dict) -> List[Dict[str, Any]]:
    """
    Run the value in the dwc:locality field through the Mindat Locality API and return the highest result per occurrence
    :param specimen_data: The full specimen object
    :return: List of the results including some metadata
    """
    occurrences = specimen_data.get('occurrences')
    result_list = list()
    for index, occurrence in enumerate(occurrences):
        if occurrence.get('location') is not None:
            querystring = f"https://api.mindat.org/localities/?txt={occurrence['location']['dwc:locality']}"
            response = requests.get(querystring, headers={'Authorization': 'Token ' + os.environ.get('API_KEY')})
            response_json = json.loads(response.content)
            if len(response_json) == 0:
                logging.info("No results for this locality where found: " + querystring)
            else:
                logging.info('Highest hit is: ' + json.dumps(response_json.get('results')[0], indent=2))
                result_list.append(
                    {'queryString': querystring, 'geo_reference_result': response_json.get('results')[0],
                     'occurrence_index': index})
    return result_list


def run_local(example: str):
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/W32-FLA-P8V
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen = json.loads(response.content)['data']
    specimen_data = specimen['attributes']['digitalSpecimen']
    result = run_georeference(specimen_data)
    mas_job_record = map_to_mas_job_record(specimen_data, result, str(uuid.uuid4()))
    logging.info('Created annotations: ' + str(mas_job_record))


if __name__ == '__main__':
    start_kafka()
    # run_local('https://dev.dissco.tech/api/v1/specimens/TEST/W32-FLA-P8V')
