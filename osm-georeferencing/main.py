import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Union, Tuple

import requests
from kafka import KafkaConsumer, KafkaProducer
from shapely import from_geojson

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
ODS_TYPE = "ods:type"
ODS_ID = "ods:id"
LOCATION_PATH = "digitalSpecimenWrapper.ods:attributes.occurrences[*].location."
USER_AGENT = "Distributed System of Scientific Collections"


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
        try:
            logging.info('Received message: ' + str(msg.value))
            json_value = msg.value
            specimen_data = json_value['object']['digitalSpecimen']
            result, batch_metadata = run_georeference(specimen_data)
            annotation_event = map_to_annotation_event(specimen_data, result, json_value["jobId"], json_value['batchingRequested'], batch_metadata)
            send_updated_opends(annotation_event, producer)
        except Exception as e:
            logging.error(e)


def map_to_annotation_event(specimen_data: Dict, results: List[Dict[str, str]], job_id: str, batching: bool, batch_metadata: List[Dict]) -> Dict:
    """
    Map the result of the API call to a mas job record
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the geoCASe identifier
    :param job_id: The job ID of the MAS
    :param batching: batch functionality was requested by scheduling user
    :param batch_metadata: metadata to facilitate batching downstream
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = timestamp_now()
    if results is None:
        annotations = list()
    else:
        annotations = list(map(lambda result: map_to_annotation(specimen_data, result, timestamp, batching), results))
    annotation_event = {
        "jobId": job_id,
        "annotations": annotations
    }
    if batching:
        annotation_event["batchMetadata"] = batch_metadata
    return annotation_event


def map_to_annotation(specimen_data: Dict, result: Dict[str, Any], timestamp: str, batching: bool) -> Dict:
    """
    Map the result of the Locality API call, to a georeference annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param result: The result of the Locality API call
    :param timestamp: The current timestamp
    :param batching: batch functionality was requested by scheduling user
    :return: A single annotation with the georeference information from the locality
    """
    if result['is_point']:
        point_coordinate = result['osm_result']['geometry']
    else:
        point_coordinate = {
           'coordinates': [
               result['geopick_result']['decimalLongitude'],
               result['geopick_result']['decimalLatitude']
           ]
        }
    oa_value = {
        "georeference": {
            "dwc:decimalLatitude": round(point_coordinate['coordinates'][1], 7),
            "dwc:decimalLongitude": round(point_coordinate['coordinates'][0], 7),
            "dwc:geodeticDatum": 'epsg:4326',
            "dwc:coordinateUncertaintyInMeters": None if result['is_point'] else
            result['geopick_result']['coordinateUncertaintyInMeters'],
            "dwc:pointRadiusSpatialFit": None if result['is_point'] else result['geopick_result']['pointRadiusSpatialFit'],
            "dwc:coordinatePrecision": 0.0000001,
            "dwc:footprintSRS": 'epsg:4326',
            "dwc:footprintWKT": from_geojson(json.dumps(result.get('osm_result').get('geometry'))).wkt,
            "dwc:footprintSpatialFit": None if result['is_point'] else 1,
            "dwc:georeferencedBy": f"https://hdl.handle.net/{os.environ.get('MAS_ID')}",
            "dwc:georeferencedDate": timestamp,
            "dwc:georeferenceSources": 'GeoPick v.1.0.4',
            "dwc:georeferenceProtocol": 'Georeferencing Quick Reference Guide (Zermoglio et al. 2020, '
                                        'https://doi.org/10.35035/e09p-h128)',
            "dwc:georeferenceRemarks": f"This georeference was created by the GeoPick API. Based on OpenStreetMap API "
                                       f"query of {result['queryString']}"
        }
    }

    return wrap_oa_value(oa_value, result, specimen_data, timestamp,
                         f"$.occurrences[{result['occurrence_index']}].location.georeference", batching)


def wrap_oa_value(oa_value: Dict, result: Dict[str, Any], specimen_data: Dict, timestamp: str, oa_class: str, batching: bool) -> Dict:
    """
    Generic method to wrap the oa_value into an annotation object
    :param oa_value: The value that contains the result of the MAS
    :param result: The result of the Locality API call
    :param specimen_data: The JSON value of the Digital Specimen
    :param timestamp: The current timestamp
    :param oa_class: The name of the class to which the class annotation points
    :param batching: batch functionality was requested
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
    if batching:
        annotation['placeInBatch'] = result['occurrence_index']
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


def send_updated_opends(annotation: Union[None, Dict], producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info('Publishing annotation: ' + str(annotation))
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation)


def run_georeference(specimen_data: Dict) -> Tuple[List[Dict[str, Any]], List[Dict]]:
    """
    Calls georeference APIs. First of Open Street Map to get the initial georeference of the locality.
    If response is a point location it is immediately returned.
    If the response is not a point location we will call the GeoPick API to get the centroid of the polygon.
    :param specimen_data: The Digital Specimen data
    :return: Returns a list of all the results. This could be multiple as we can have more than one occurrence
    per specimen. Also returns the batch metadata.
    """
    occurrences = specimen_data.get('occurrences')
    result_list = list()
    batch_metadata = []
    for index, occurrence in enumerate(occurrences):
        if occurrence.get('location') is not None:
            location = occurrence.get('location')
            query_string, batch_metadata_unit = build_query_string(location, index)
            headers = {
                'User-Agent': USER_AGENT
            }
            response = requests.get(query_string, headers=headers)
            response_json = response.json()
            if len(response_json.get('features')) == 0:
                logging.info("No results for this locality where found: " + query_string)
            else:
                batch_metadata.append(batch_metadata_unit)
                first_feature = response_json.get('features')[0]
                logging.info('Highest hit is: ' + json.dumps(first_feature))
                result = {'queryString': query_string,
                          'osm_result': first_feature,
                          'occurrence_index': index}

                if first_feature.get('geometry').get('type') != 'Point':
                    result['geopick_result'] = run_geopick_api(first_feature)
                    result['is_point'] = False
                else:
                    result['is_point'] = True
                result_list.append(result)
    return result_list, batch_metadata


def build_query_string(location: Dict, index: int) -> Tuple[str, Dict]:
    """
    Builds query string for OSM georeferencing service
    :param location: Location object of the DigitalSpecimen, stored in Occurrences[*].location
    :param index: Array index of the Occurrence
    :return: query string (str) for API and batch metadata (Dict)
    """
    batch_metadata = {
        'placeInBatch': index,
        'searchParams': [
            {
                'inputField': LOCATION_PATH + 'dwc:locality',
                'inputValue': location['dwc:locality']
            }
        ]
    }

    querystring = f"https://nominatim.openstreetmap.org/search.php?q={location['dwc:locality']}"
    for field_name in ['dwc:municipality', 'dwc:county', 'dwc:stateProvince', 'dwc:country']:
        next_field, search_param = get_supporting_info(field_name, location)
        querystring += next_field
        batch_metadata['searchParams'].append(search_param)
        querystring += '&format=geojson&polygon_geojson=1'
    return querystring, batch_metadata


def get_supporting_info(field_name: str, location: Dict) -> Tuple[str, Dict]:
    """
    Get the supporting information from the specimen data
    :param field_name: The name of the field used to build the Query String for the OSM georeferencing API. One of: 'dwc:municipality', 'dwc:county', 'dwc:stateProvince', 'dwc:country'
    :param location: The JSON value of the Digital Specimen
    :return: The value of the field and the batchMetadata search params
    """
    if location.get(field_name) is None:
        return '', build_batch_metadata_search_param(field_name, '')
    else:
        return ', ' + location.get(field_name), build_batch_metadata_search_param(field_name, location.get(field_name))


def build_batch_metadata_search_param(field_name: str, field_val: str) -> Dict:
    """
    :param field_name: field name of the Location
    :param field_val: Value of the above field
    :return: Search param for
    """
    return {
        'inputField': LOCATION_PATH + field_name,
        'inputValue': field_val
    }


def run_geopick_api(feature) -> Dict:
    """
    Call the GeoPick API to get the centroid of the polygon
    :param feature: The geojson feature
    :return: The result of the GeoPick API call
    """
    querystring = 'https://geopick.gbif.org/v1/georeference-dwc'
    response = requests.post(querystring, json=feature, headers=get_geopick_auth())
    response_json = response.json()
    return response_json


def get_geopick_auth():
    """
    Retrieves token for Geopick authorization
    :return: Authorization header
    """
    auth_info = {
        'username':  os.environ.get('GEOPICK_USER'),
        'password': os.environ.get('GEOPICK_PWD')
    }
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT
    }
    response = requests.post('https://geopick.gbif.org/v1/authenticate',
                             json=auth_info,
                             headers=headers)
    response.raise_for_status()
    return {
        'Authorization': 'Bearer ' + response.json()['token']
    }


def run_local(example: str):
    """
    Run the script locally. Can be called by replacing the kafka call with this  a method call in the main method.
    Will call the DiSSCo API to retrieve the specimen data.
    A record ID will be created but can only be used for testing.
    :param example: The full URL of the Digital Specimen to the API (for example
    https://dev.dissco.tech/api/v1/specimens/TEST/65V-T1W-1PD)
    :return: Return nothing but will log the result
    """
    response = requests.get(example)
    specimen = json.loads(response.content)['data']
    specimen_data = specimen['attributes']['digitalSpecimen']
    result, batch_metadata = run_georeference(specimen_data)
    mas_job_record = map_to_annotation_event(specimen_data, result, str(uuid.uuid4()), True, batch_metadata)
    logging.info('Created annotations: ' + json.dumps(mas_job_record, indent=2))


if __name__ == '__main__':
    #start_kafka()
    run_local('https://dev.dissco.tech/api/v1/specimens/TEST/65V-T1W-1PD')
