import json
import logging
import os
import string
from datetime import datetime, timezone
from typing import Dict, Any
import requests

from kafka import KafkaConsumer, KafkaProducer

ODS_ID = 'ods:id'
ODS_TYPE = 'ods:type'


def start_kafka() -> None:
  """
  Start a kafka listener and process the messages by unpacking the image.
  When done it will republish the object, so it can be validated and storage by the processing service
  :param name: The topic name the Kafka listener needs to listen to
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
    annotation_event = run_date_check(json_value['object']['digitalSpecimen'],
                                      json_value['batchingRequested'],
                                      json_value["jobId"])
    send_to_kafka(annotation_event, producer)


def send_to_kafka(annotation_event: Dict, producer: KafkaProducer) -> None:
  """
  Send the annotation to the Kafka topic
  :param annotation_event: The formatted annotation event
  :param producer: The initiated Kafka producer
  :return: Will not return anything
  """
  logging.info('Publishing annotation event: ' + str(annotation_event))
  producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation_event)


def run_local() -> Dict:
  specimen = requests.get(
    "https://dev.dissco.tech/api/v1/specimens/TEST/VEB-VF3-09K").json()['data'][
    'attributes']['digitalSpecimen']
  event = run_date_check(specimen, True, "TEST/1234")
  logging.info(event)


def run_date_check(specimen_data: Dict, batching_requested: bool,
    job_id: str) -> Dict:
  occurrences = specimen_data['occurrences']
  place_in_batch = 0
  annotation_event = {
    'jobId': job_id,
    'annotations': []
  }
  if batching_requested:
    annotation_event['batchMetadata'] = []
  for i in range(0, len(occurrences)):
    date = occurrences[i]['dwc:eventDate']
    if date is not None and correct_format(date):
      target_field = "['digitalSpecimen']['occurrences'][" + str(
        i) + "]['dwc:eventDate']"
      annotation = map_to_annotation(specimen_data[ODS_ID], target_field)
      annotation_event['annotations'].append(annotation)
      if batching_requested:
        batch_metadata = build_batch_metadata(date, place_in_batch)
        annotation_event['batchMetadata'].append(batch_metadata)
        place_in_batch = place_in_batch + 1
  return annotation_event


def build_batch_metadata(date_value, place_in_batch: int) -> Dict[str, Any]:
  batch_metadata = {
    "placeInBatch": place_in_batch,
    "inputField": "digitalSpecimenWrapper.ods:attributes.occurrences[*].ods:eventDate",
    "inputValue": date_value
  }
  return batch_metadata


def map_to_annotation(pid: str, target_field: str) -> dict:
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
      ODS_ID: 'https://hdl.handle.net/' + pid,
      ODS_TYPE: 'https://doi.org/21.T11148/894b1e6cad57e921764e',
      'oa:selector': {
        ODS_TYPE: 'FieldSelector',
        'ods:field': target_field
      },
    },
    'oa:body': {
      ODS_TYPE: 'oa:TextualBody',
      'oa:value': [
        'EventDate is in YYYY-MM-DD'
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
