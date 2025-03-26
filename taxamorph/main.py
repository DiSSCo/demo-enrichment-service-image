import json
import logging
import os
from typing import Dict, List, Any
import requests
from kafka import KafkaConsumer, KafkaProducer
import requests_cache
import shared 
import uuid


# Enable caching for both GET and POST requests
requests_cache.install_cache(
    'taxamorph',
    expire_after=3600,  # Cache for 1 hour
    allowable_methods=["GET", "POST"]  # Include POST requests in the cache
)
 
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
 
TAXAMORPH_ENDPOINT = 'https://merry-malamute-bold.ngrok-free.app/infer'
 
def start_kafka() -> None:
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
            digital_media = json_value.get("object")

            taxamorph_result = run_api_call(digital_media)

            annotations = map_result_to_annotation(
                digital_media, taxamorph_result
            )

            event = map_to_annotation_event(annotations, json_value.get("jobId"))
            publish_annotation_event(event, producer)

        except Exception as e:
            logging.exception(e)
 
def map_to_annotation_event(annotations: List[Dict], job_id: str) -> Dict:
    return {"annotations": annotations, "jobId": job_id}

 
def map_result_to_annotation(
    digital_media: Dict,
    taxamorph_result: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """

    """
    
    timestamp = shared.timestamp_now()
    ods_agent = shared.get_agent()

    if taxamorph_result:
        annotations = list()

        for result in taxamorph_result:

            oa_value = shared.map_to_entity_relationship(
                "hasTaxaMorphDownloadURL",
                result["downloadURL"],
                result["downloadURL"],
                timestamp,
                ods_agent,
            )    

            oa_selector = shared.build_class_selector(shared.ER_PATH)

            annotation = shared.map_to_annotation(
                ods_agent,
                timestamp,
                oa_value,
                oa_selector,
                digital_media[shared.ODS_ID],
                digital_media[shared.ODS_TYPE],
                result["downloadURL"],
            )
            annotations.append(annotation)
    else:

        annotations = [
            shared.map_to_empty_annotation(
                timestamp, "No results for TaxaMorph", digital_media, shared.ER_PATH
            )
        ]        

    return annotations
 
def publish_annotation_event(annotation_event: Dict, producer: KafkaProducer) -> None:
    logging.info("Publishing annotation: " + str(annotation_event))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)
 
def run_api_call(digital_media: Dict) -> List[Dict[str, str]]:

    data = {
        "jobId": "20.5000.1025/AAA-111-BBB",
        "object": {
            "digitalSpecimen": {
                "@id": "https://doi.org/10.3535/XYZ-XYZ-XYZ",
                "dwc:scientificName": "Example species",
                "dwc:taxonID": "123456",
                "media": [
                    digital_media
                ]
            }
        },
        "batchingRequested": False
    } 
 
    try:
        response = requests.post(TAXAMORPH_ENDPOINT, json=data)
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return []
 
    annotations = result.get("annotations", [])
    if not annotations:
        logging.error("No annotations found in the response.")
        return []
 
    download_url = annotations[0].get("oa:hasBody", {}).get("oa:value")

    if not download_url:
        logging.error("No download URL found in the response.")
        return []
    
    logging.debug(f"Download image url:\n{download_url}")  
 
    return [{
        "downloadURL": download_url,
        "processid": "TaxaMorph"
    }]
 
def run_local(media_id: str) -> None:
    """
    Runs script locally. Demonstrates using a specimen target
    :param specimen_id: A specimen ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_media = (
        requests.get(
            f"https://sandbox.dissco.tech/api/digital-media/v1/{media_id}"
        )
        .json()
        .get("data")
        .get("attributes")
    )

    taxamorph_result = run_api_call(digital_media)

    annotations = map_result_to_annotation(
        digital_media, taxamorph_result
    )

    event = map_to_annotation_event(annotations, str(uuid.uuid4()))    

    logging.info("Created annotations: " + json.dumps(event, indent=2))




if __name__ == "__main__":
    run_local('SANDBOX/4LB-38S-KSM')
    # start_kafka()