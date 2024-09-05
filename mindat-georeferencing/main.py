import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple

import requests
from kafka import KafkaConsumer, KafkaProducer
from shared import *

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'),
                             group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[
                                 os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(
                                 m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
        value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        try:
            logging.info('Received message: ' + str(msg.value))
            json_value = msg.value
            mark_job_as_running(json_value["jobId"])
            specimen_data = json_value['object']['attributes']
            batching_requested = json_value['batchingRequested']
            result, batch_metadata = run_georeference(specimen_data,
                                                      batching_requested)
            mas_job_record = map_to_annotation_event(specimen_data, result,
                                                     json_value["jobId"],
                                                     batch_metadata)
            send_updated_opends(mas_job_record, producer)
        except Exception as e:
            logging.exception(e)


def mark_job_as_running(job_id: str):
    """
    Calls DiSSCo's RUNNING endpoint to inform the system that the message has
    been received by the MAS. Doing so will update the status of the job to
    "RUNNING" for any observing users.
    :param job_id: the job id from the kafka message
    """
    query_string = os.environ.get('RUNNING_ENDPOINT') + os.environ.get(
        'MAS_ID') + '/' + job_id + '/running'
    requests.get(query_string)


def map_to_annotation_event(specimen_data: Dict, results: List[Dict[str, str]],
        job_id: str, batch_metadata: List[Dict[str, Any]]) -> Dict:
    """
    Map the result of the API call to an annotation
    :param batch_metadata: Information about the computation, if requested
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = timestamp_now()
    batching_requested = len(batch_metadata) > 0
    ods_agent = get_agent()
    if results is None:
        annotations = list()
    else:
        annotations = list(map(
            lambda result: map_to_georeference_annotation(specimen_data, result,
                                                          timestamp,
                                                          batching_requested,
                                                          ods_agent),
            results))
        annotations.extend(list(map(
            lambda result: map_to_entity_relationship_annotation(specimen_data,
                                                                 result,
                                                                 timestamp,
                                                                 batching_requested,
                                                                 ods_agent),
            results)))
    annotation_event = {
        "jobId": job_id,
        "annotations": annotations
    }
    if batch_metadata:
        annotation_event["batchMetadata"] = batch_metadata
    return annotation_event


def build_batch_metadata(locality: str, place_in_batch: int) -> Dict[str, Any]:
    batch_metadata = {
        "ods:placeInBatch": place_in_batch,
        "searchParams": [
            {
                "inputField": "$.ods:hasEvent[*].ods:Location.dwc:locality",
                "inputValue": locality
            }
        ]
    }
    return batch_metadata


def map_to_entity_relationship_annotation(specimen_data: Dict,
        result: Dict[str, Any], timestamp: str, batching_requested: bool,
        ods_agent: Dict) -> Dict:
    """
    Map the result of the Mindat Locality API call to an entityRelationship annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Mindat Locality API call
    :param timestamp: The current timestamp
    :param batching_requested: Indicates if the scheduling party requested batching
    :return:  A single annotation with the relationship to the Mindat locality
    """
    oa_value = map_to_entity_relationship(
        'hasMindatLocation',
        f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
        timestamp, ods_agent
    )
    return wrap_oa_value(oa_value, result, specimen_data, timestamp,
                         '$.ods:hasEntityRelationship', batching_requested,
                         ods_agent)


def map_to_georeference_annotation(specimen_data: Dict, result: Dict[str, Any],
        timestamp: str, batching_requested: bool, ods_agent: Dict) -> Dict:
    """
    Map the result of the Mindat Locality API call to a georeference annotation
    :param batching_requested: Indicates if the scheduling party requested batching
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Mindat Locality API call
    :param timestamp: The current timestamp
    :return: A single annotation with the georeference information from the Mindat locality
    """
    oa_value = {
        "dwc:decimalLatitude": round(
            result['geo_reference_result']['latitude'],
            7),
        "dwc:decimalLongitude": round(
            result['geo_reference_result']['longitude'],
            7),
        "dwc:geodeticDatum": 'WGS84',
        "dwc:georeferencedBy": f"https://hdl.handle.net/{os.environ.get('MAS_ID')}",
        "dwc:georeferencedDate": timestamp,
        "dwc:georeferenceSources": f"https://www.mindat.org/loc-{result['geo_reference_result']['id']}.html",
        "dwc:georeferenceProtocol": "Georeferenced against the Mindat Locality API based on the specimen "
                                    "locality string (dwc:locality)"
    }

    return wrap_oa_value(oa_value, result, specimen_data, timestamp,
                         f"$.ods:hasEvent[{result['result_index']}].ods:Location.ods:GeoReference",
                         batching_requested, ods_agent)


def wrap_oa_value(oa_value: Dict, result: Dict[str, Any], specimen_data: Dict,
        timestamp: str, oa_class: str, batching_requested: bool,
        ods_agent: Dict) -> Dict:
    """
    Generic method to wrap the oa_value into an annotation object
    :param batching_requested: Indicates if the scheduling party requested batching
    :param oa_value: The value that contains the result of the MAS
    :param result: The result of the Mindat Locality API call
    :param specimen_data: The JSON value of the Digital Specimen
    :param timestamp: The current timestamp
    :param oa_class: The name of the class to which the class annotation points
    :return: Returns an annotation with all the relevant metadata
    """
    oa_selector = build_class_selector(oa_class)
    annotation = map_to_annotation(
        ods_agent, timestamp, oa_value, oa_selector, specimen_data[ODS_ID],
        specimen_data[ODS_TYPE],
        result['queryString']
    )
    # If batching is requested, the annotation must contain a "placeInBatch" value equal to the corresponding batch metadata
    if batching_requested:
        annotation['ods:placeInBatch'] = result["result_index"]

    return annotation


def send_updated_opends(annotation: Dict, producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info('Publishing annotation: ' + str(annotation))
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation)


def run_georeference(specimen_data: Dict, batching_requested: bool) -> Tuple[
    List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Run the value in the dwc:locality field through the Mindat Locality API and return the highest result per occurrence
    :param batching_requested: Indicates if the scheduling party requested batching
    :param specimen_data: The full specimen object
    :return: List of the results including some metadata
    """
    events = specimen_data.get('ods:hasEvent')
    result_list = list()
    batch_metadata = list()
    for index, event in enumerate(events):
        if (event.get('ods:Location') is not None
            and event.get("ods:Location").get("dwc:locality")) is not None:
            location = event.get("ods:Location")
            querystring = f"https://api.mindat.org/localities/?txt={location.get('dwc:locality')}"
            response = requests.get(querystring, headers={
                'Authorization': 'Token ' + os.environ.get('API_KEY')})
            logging.info("Response from mindat status code: " + str(
                response.status_code))
            logging.info("Response from mindat" + str(response.content))
            response_json = json.loads(response.content)
            if not response_json:
                logging.info(
                    "No results for this locality where found: " + querystring)
            else:
                logging.info(
                    'Highest hit is: ' + json.dumps(
                        response_json.get('results')[0],
                        indent=2))
                result_list.append(
                    {'queryString': querystring,
                     'geo_reference_result': response_json.get('results')[0],
                     'result_index': index})
                if batching_requested:
                    batch_metadata.append(
                        build_batch_metadata(
                            event['ods:Location']['dwc:locality'],
                            index))
    return result_list, batch_metadata


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
    specimen_data = json.loads(response.content)['data']['attributes']
    result, batch_metadata = run_georeference(specimen_data, True)
    annotation_event = map_to_annotation_event(specimen_data, result,
                                               str(uuid.uuid4()),
                                               batch_metadata)
    logging.info(
        'Created annotations: ' + json.dumps(annotation_event, indent=2))


if __name__ == '__main__':
    # start_kafka()
    run_local(
        'https://dev.dissco.tech/api/v1/digital-specimen/TEST/VGJ-1R7-JSJ')
