import json
import logging
import os
import string
from datetime import datetime, timezone
from typing import Dict, Tuple, List
import requests

from kafka import KafkaConsumer, KafkaProducer

ODS_ID = 'ods:id'
ODS_TYPE = 'ods:type'


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
    json_value = msg.value
    logging.info(json_value)
    annotation_event = run_annotations(json_value)
    send_to_kafka(annotation_event, producer)

def run_annotations(json_value: Dict) -> Dict :
  job_id = json_value['jobId']
  specimen_data = json_value['object']['digitalSpecimen']
  batching_requested = json_value['batchingRequested']
  date_annotation, date_batch, place_in_batch = run_date_check(
    specimen_data,
    batching_requested)
  lin_annotation, lin_batch = run_linnaeus_check(specimen_data, batching_requested, place_in_batch)
  annotations = date_annotation + lin_annotation
  batch_metadata = date_batch + lin_batch
  event = {
    'jobId': job_id,
    'annotations': annotations,
    'batchMetadata': batch_metadata
  }
  return event



def send_to_kafka(annotation_event: Dict, producer: KafkaProducer) -> None:
  """
  Send the annotation to the Kafka topic
  :param annotation_event: The formatted annotation event
  :param producer: The initiated Kafka producer
  :return: Will not return anything
  """
  logging.info('Publishing annotation event: ' + str(annotation_event))
  producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation_event)


def run_local() -> None:
  specimen = requests.get(
    "https://dev.dissco.tech/api/v1/specimens/TEST/VEB-VF3-09K").json()['data'][
    'attributes']['digitalSpecimen']
  message = {
    'object': {
      'digitalSpecimen' : specimen
    },
    'jobId':'111',
    'batchingRequested': True
  }
  event = run_annotations(message)
  logging.info(event)


def run_date_check(specimen_data: Dict, batching_requested: bool) -> Tuple[
  List[Dict], List[Dict], int]:
  occurrences = specimen_data['occurrences']
  place_in_batch = 0
  batch_metadata = []
  annotations = []
  for i in range(0, len(occurrences)):
    date = occurrences[i]['dwc:eventDate']
    if date is not None and correct_format(date):
      target_field = "digitalSpecimenWrapper.ods:attributes.occurrences[" + str(
        i) + "].dwc:eventDate"
      annotation = map_to_annotation(specimen_data[ODS_ID], target_field, 'EventDate is in YYYY-MM-DD')
      if batching_requested:
        annotation['placeInBatch'] = place_in_batch
        batch_metadata.append(build_batch_metadata(target_field, date, place_in_batch))
        place_in_batch = place_in_batch + 1
      annotations.append(annotation)
  return annotations, batch_metadata, place_in_batch

def run_linnaeus_check(specimen_data: Dict, batching_requested: bool,
    place_in_batch: int) -> Tuple[List[Dict], List[Dict]]:
  identifications = specimen_data['dwc:identification']
  annotations = []
  batch_metadata = []
  for i in range(0, len(identifications)):
    taxon_ids = identifications[i]['taxonIdentifications']
    for j in range(0, len(taxon_ids)):
      if " (L.) " in taxon_ids[j]['dwc:scientificName']:
        text = "This taxonomic authority for this identification is Linnaeus"
        target_field = "digitalSpecimenWrapper.ods:attributes.dwc:identification[" + str(
          i) + "].taxonIdentifications[" + str(j) + "].dwc:scientificName"
        annotation = map_to_annotation(specimen_data[ODS_ID], target_field, text)
        if batching_requested:
          annotation['placeInBatch'] = place_in_batch
          batch_metadata.append(build_batch_metadata(target_field, taxon_ids[j]['dwc:scientificName'], place_in_batch))
          place_in_batch = place_in_batch + 1
        annotations.append(annotation)
  return annotations, batch_metadata

def build_batch_metadata(input_field, input_value, place_in_batch: int) -> Dict:
  batch_metadata = {
    "placeInBatch": place_in_batch,
    "inputField": input_field,
    "inputValue": input_value
  }
  return batch_metadata


def map_to_annotation(pid: str, target_field: str, value: str) -> dict:
  annotation = {
    'rdf:type': 'Annotation',
    'oa:motivation': 'oa:assessing',
    'oa:creator': {
      ODS_TYPE: 'oa:SoftwareAgent',
      'foaf:name': 'Demo batch MAS',
      ODS_ID: 'https://hdl.handle.net/demo-batch-mas'
    },
    'dcterms:created': timestamp_now(),
    'oa:target': {
      ODS_ID: pid,
      ODS_TYPE: 'https://doi.org/21.T11148/894b1e6cad57e921764e',
      'oa:selector': {
        ODS_TYPE: 'FieldSelector',
        'ods:field': target_field
      },
    },
    'oa:body': {
      ODS_TYPE: 'oa:TextualBody',
      'oa:value': [
        value
      ]
    }
  }
  logging.info(annotation)
  return annotation


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


def correct_format(date: str) -> bool:
  if string_is_empty(date):
    return False
  date_split = date.split("-")
  if (len(date_split)) != 3:
    return False
  if len(date_split[0]) == 4 and len(date_split[1]) <= 2 and len(
      date_split[2]) <= 2:
    if (date_split[1].isdigit() and int(date_split[1]) <= 12):
      return True
  return False


def string_is_empty(param: string) -> bool:
  param = param.strip()
  if param == '':
    return True
  if param.upper() == 'NULL':
    return True
  return False


if __name__ == '__main__':
  start_kafka()
