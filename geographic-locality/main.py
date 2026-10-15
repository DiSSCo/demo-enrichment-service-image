"""Geographic Locality MAS: Indigenous community context, Sámi (prototype).

RabbitMQ message -> value service (embedded, or the API at VALUE_SERVICE_URL) ->
one oa:commenting per location: may a Sámi community be connected to this locality,
and whom should the holding institution contact for collaboration and approval ->
mas-annotation-exchange. A failed name-register lookup (with no listed area), or a
comment that would contain a name, sends a failure message instead (job FAILED).
See README.md.

Local dry run: prints the annotation events, publishes nothing.
  PYTHONPATH=. venv/bin/python geographic-locality/main.py SANDBOX/X16-9NF-V0S [SANDBOX/... ...]
  PYTHONPATH=. venv/bin/python geographic-locality/main.py path/to/specimen.json
"""

import json
import logging
import os
import sys
import uuid
from typing import Any

import community_annotations
import pika
import requests
import value_service
from pika.adapters.blocking_connection import BlockingChannel
from pika.amqp_object import Method, Properties

import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
SANDBOX_API = "https://sandbox.dissco.tech/api/digital-specimen/v1"
PREVIEW_MAS_ID = "PREVIEW/GEOGRAPHIC-LOCALITY"
PREVIEW_MAS_NAME = "Geographic Locality: Indigenous community context, Sámi (prototype)"


def run_rabbitmq() -> None:
    """Consume Digital Specimens from the MAS queue, one message at a time."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            os.environ.get("RABBITMQ_HOST"),
            credentials=pika.PlainCredentials(os.environ.get("RABBITMQ_USER"), os.environ.get("RABBITMQ_PASSWORD")),
        )
    )
    channel = connection.channel()
    channel.basic_consume(queue=os.environ.get("RABBITMQ_QUEUE"), on_message_callback=process_message, auto_ack=True)
    channel.start_consuming()


def process_message(channel: BlockingChannel, method: Method, properties: Properties, body: bytes) -> None:
    """Annotate one specimen and publish the event; on any error send a failure message."""
    json_value = json.loads(body.decode("utf-8"))
    job_id = json_value.get("jobId")
    try:
        shared.mark_job_as_running(job_id)
        annotation_event = build_annotation_event(json_value.get("object"), job_id)
        publish_annotation_event(annotation_event, channel)
    except Exception as e:  # noqa: BLE001 - any error must end the job as FAILED, never leave it RUNNING
        shared.send_failed_message(job_id, str(e), channel)


def build_annotation_event(specimen: dict[str, Any], job_id: str) -> dict[str, Any]:
    """Value service (with its names guard) -> community comments -> annotation event."""
    response = value_service.enrich(specimen)
    annotations = community_annotations.build_annotations(
        specimen, response, shared.get_agent(), shared.timestamp_now()
    )
    return {"jobId": job_id, "annotations": annotations}


def publish_annotation_event(annotation_event: dict[str, Any], channel: BlockingChannel) -> None:
    """Send the annotation event to DiSSCo's annotation processing service."""
    logger.info("Publishing annotation event: %s", json.dumps(annotation_event, ensure_ascii=False))
    channel.basic_publish(
        exchange=os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-exchange"),
        routing_key=os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation"),
        body=json.dumps(annotation_event).encode("utf-8"),
    )


def fetch_specimen(ref: str) -> dict[str, Any]:
    """A Digital Specimen from a JSON file, a full API URL or a Sandbox ID (SANDBOX/...)."""
    if os.path.isfile(ref):
        with open(ref, encoding="utf-8") as f:
            data = json.load(f)
    else:
        url = ref if ref.startswith("http") else f"{SANDBOX_API}/{ref}"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
    return (data.get("data") or {}).get("attributes") or data


def run_local(refs: list[str]) -> list[dict[str, Any]]:
    """Dry run: build the annotation events for some specimens and print them. Publishes nothing."""
    shared.shared.MAS_ID = shared.shared.MAS_ID or PREVIEW_MAS_ID
    shared.shared.MAS_NAME = shared.shared.MAS_NAME or PREVIEW_MAS_NAME
    events = [build_annotation_event(fetch_specimen(ref), f"dry-run-{uuid.uuid4()}") for ref in refs]
    print(json.dumps(events, indent=2, ensure_ascii=False))
    return events


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_local(sys.argv[1:])
    else:
        run_rabbitmq()
