import json
import logging
import os

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and storage by the processing service
    :param name: The topic name the Kafka listener needs to listen to
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'), group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        logging.info(msg.value)
        digital_specimen_record = msg.value
        check_licence(digital_specimen_record, producer)
        check_space_information(digital_specimen_record, producer)
        check_dates(digital_specimen_record, producer)


def check_dates(digital_specimen_record, producer):
    data = digital_specimen_record.get('digitalSpecimen').get('data')
    if data.get('dwc:eventDate') is None or string_is_empty(data.get('dwc:eventDate')):
        annotation_body = {
            'value': 'EventDate is empty which does not comply to '
                     'TDWG data quality check: f51e15a6-a67d-4729-9c28-3766299d2985'
        }
        annotation = map_to_annotation(digital_specimen_record.get('id'), 'eventDate', annotation_body)
        publish_annotation(annotation, producer)


def check_licence(digital_specimen_record, producer) -> None:
    data = digital_specimen_record.get('digitalSpecimen').get('data')
    licence = data.get('dcterms:license')
    if licence is None or string_is_empty(licence):
        annotation_body = {
            'value': 'Licence is empty which does not comply to '
                     'TDWG data quality check: 15f78619-811a-4c6f-997a-a4c7888ad849'
        }
        annotation = map_to_annotation(digital_specimen_record.get('id'), 'license', annotation_body)
        publish_annotation(annotation, producer)
        return

    approved_licences = ['CC BY',
                         'https://creativecommons.org/licenses/by/4.0/legalcode',
                         'https://creativecommons.org/licenses/by/4.0/',
                         'http://creativecommons.org/licenses/by/4.0/'
                         'http://creativecommons.org/licenses/by/4.0/legalcode',
                         'CC0',
                         'https://creativecommons.org/publicdomain/zero/1.0/',
                         'http://creativecommons.org/publicdomain/zero/1.0/legalcode',
                         'https://creativecommons.org/publicdomain/zero/1.0/legalcode'
                         'http://creativecommons.org/publicdomain/zero/1.0/'
                         'CC BY-SA',
                         'https://creativecommons.org/licenses/by-sa/4.0/',
                         'http://creativecommons.org/licenses/by-sa/4.0/',
                         'https://creativecommons.org/licenses/by-sa/4.0/legalcode',
                         'http://creativecommons.org/licenses/by-sa/4.0/legalcode',
                         'CC BY-NC',
                         'https://creativecommons.org/licenses/by-nc/4.0/',
                         'http://creativecommons.org/licenses/by-nc/4.0/',
                         'https://creativecommons.org/licenses/by-nc/4.0/legalcode',
                         'http://creativecommons.org/licenses/by-nc/4.0/legalcode'
                         'CC BY-NC-SA',
                         'https://creativecommons.org/licenses/by-nc-sa/4.0/',
                         'http://creativecommons.org/licenses/by-nc-sa/4.0/',
                         'https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode',
                         'http://creativecommons.org/licenses/by-nc-sa/4.0/legalcode',
                         'CC BY-ND',
                         'https://creativecommons.org/licenses/by-nd/4.0/',
                         'http://creativecommons.org/licenses/by-nd/4.0/',
                         'https://creativecommons.org/licenses/by-nd/4.0/legalcode',
                         'http://creativecommons.org/licenses/by-nd/4.0/legalcode',
                         'CC BY-NC-ND',
                         'https://creativecommons.org/licenses/by-nc-nd/4.0/',
                         'http://creativecommons.org/licenses/by-nc-nd/4.0/'
                         'https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode',
                         'http://creativecommons.org/licenses/by-nc-nd/4.0/legalcode'
                         ]
    if licence not in approved_licences:
        annotation_body = {
            'value': 'Licence is not of a format or type approved by the '
                     'TDWG data quality check: 3136236e-04b6-49ea-8b34-a65f25e3aba1. '
                     'See https://creativecommons.org/about/cclicenses/ for correct formats'
        }
        annotation = map_to_annotation(digital_specimen_record.get('id'), 'license', annotation_body)
        publish_annotation(annotation, producer)


def check_space_information(digital_specimen_record, producer):
    dwc_location_fields = ['dwc: locationID', 'dwc: higherGeographyID', 'dwc: higherGeography', 'dwc: continent',
                           'dwc: country', 'dwc: countryCode', 'dwc: stateProvince',
                           'dwc: county', 'dwc: municipality', 'dwc: waterBody', 'dwc: island', 'dwc: islandGroup',
                           'dwc: locality', 'dwc: verbatimLocality', 'dwc: decimalLatitude', 'dwc: decimalLongitude',
                           'dwc: verbatimCoordinates', 'dwc: verbatimLatitude', 'dwc: verbatimLongitude',
                           'dwc: footprintWKT']
    has_location = False
    data = digital_specimen_record.get('digitalSpecimen').get('data')
    for field in dwc_location_fields:
        if data.get(field) is not None and not string_is_empty(data.get(field)):
            has_location = True
    if has_location is False:
        annotation_body = {
            'value': 'Record does not have a geolocation in any of the fields. '
                     'Record does not comply TDWG data quality check: 58486cb6-1114-4a8a-ba1e-bd89cfe887e9'
        }
        annotation = map_to_annotation(digital_specimen_record.get('id'), None, annotation_body)
        publish_annotation(annotation, producer)
        return

    if data.get('dwc:decimalLatitude') is None or string_is_empty(data.get('dwc:decimalLatitude')):
        annotation_body = {
            'value': 'Record decimal Latitude is empty. '
                     'Record does not comply TDWG data quality check: 7d2485d5-1ba7-4f25-90cb-f4480ff1a275'
        }
        annotation = map_to_annotation(digital_specimen_record.get('id'), 'decimalLatitude', annotation_body)
        publish_annotation(annotation, producer)
    else:
        decimal_latitude = data.get('dwc:decimalLatitude')
        if isinstance(decimal_latitude, str):
            decimal_latitude = int(decimal_latitude)
        if decimal_latitude > 90 or decimal_latitude < -90:
            annotation_body = {
                'value': 'Record decimal Latitude is out of range, accepted values are between -90 and 90. '
                         'Record does not comply TDWG data quality check: b6ecda2a-ce36-437a-b515-3ae94948fe83'
            }
            annotation = map_to_annotation(digital_specimen_record.get('id'), 'decimalLatitude', annotation_body)
            publish_annotation(annotation, producer)


def map_to_annotation(pid, target_field, annotation_body) -> dict:
    annotation = {
        'type': 'Annotation',
        'motivation': 'https://hdl.handle.net/quality-flagging',
        'creator': 'https://hdl.handle.net/enrichment-service-pid',
        'created': str(datetime.now(tz=timezone.utc).isoformat()),
        'target': {
            'id': 'https://hdl.handle.net/' + pid,
            'type': 'https://hdl.handle.net/21...',
        },
        'body': annotation_body
    }
    if target_field is not None:
        annotation['target']['indvProp'] = target_field
    logging.info(annotation)
    return annotation


def publish_annotation(annotation: dict, producer: KafkaProducer) -> None:
    producer.send('annotation', annotation)


def string_is_empty(param) -> bool:
    param = param.strip()
    if param == '':
        return True
    if param.upper() == 'NULL':
        return True
    return False

if __name__ == '__main__':
    start_kafka()
