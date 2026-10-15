"""DiSSCo RabbitMQ wrapper and local end-to-end job runner."""

import argparse
import base64
import ipaddress
import json
import os
import socket
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

from annotations import MEDIA_FDO, map_event

MAX_IMAGE_BYTES = 10 * 1024 * 1024


def check_media_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise ValueError("Media URL must be public HTTPS without credentials")
    addresses = socket.getaddrinfo(parts.hostname, parts.port or 443, type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(row[4][0]).is_global for row in addresses):
        raise ValueError("Private/local media addresses are not accepted")


class MediaRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        check_media_url(newurl)
        return super().redirect_request(request, fp, code, message, headers, newurl)


def fetch_image(url: str) -> bytes:
    check_media_url(url)
    with build_opener(MediaRedirect()).open(url, timeout=30) as response:
        data = response.read(MAX_IMAGE_BYTES + 1)
    if not data or len(data) > MAX_IMAGE_BYTES:
        raise ValueError("Media must be nonempty and at most 10 MiB")
    return data


def run_job(job: dict, *, image_fetch=fetch_image, inference=None) -> dict:
    if not isinstance(job, dict) or not isinstance(job.get("jobId"), str) or not job["jobId"]:
        raise ValueError("Job needs its unchanged jobId")
    media = job.get("object")
    if not isinstance(media, dict) or media.get("ods:fdoType") != MEDIA_FDO:
        raise ValueError("Expected a digital media job")
    if not isinstance(media.get("ac:accessURI"), str):
        raise ValueError("Media needs ac:accessURI")  # noqa: TRY004 - JSON job validation uses ValueError.
    if not isinstance(media.get("@id"), str) or not media["@id"].startswith("https://"):
        raise ValueError("Media needs its real HTTPS persistent identifier")
    if job.get("batchingRequested"):
        raise ValueError("Cross-resource batching is not supported for model predictions")
    data = image_fetch(media["ac:accessURI"])
    if inference is None:
        body = json.dumps({"image_base64": base64.b64encode(data).decode()}).encode()
        headers = {"Content-Type": "application/json"}
        token = os.environ.get("TRAITTRAIL_API_TOKEN")
        if token:
            headers["Authorization"] = "Bearer " + token
        endpoint = os.environ.get("TRAITTRAIL_API_URL", "http://127.0.0.1:8766").rstrip("/")
        with urlopen(Request(endpoint + "/infer", body, headers), timeout=660) as response:
            inference = json.load(response)
    return map_event(
        job, inference, os.environ["MAS_ID"], os.environ.get("MAS_NAME", "TraitTrail specimen observations")
    )


def process_message(channel, method, properties, body):
    try:
        job = json.loads(body)
        if not isinstance(job, dict):
            raise ValueError("Invalid job")  # noqa: TRY004 - JSON input boundary.
    except ValueError:
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return
    job_id = job.get("jobId")
    try:
        running = os.environ.get("RUNNING_ENDPOINT")
        if running:
            # Matches current demo repository shared.mark_job_as_running.
            url = f"{running.rstrip('/')}/{os.environ['MAS_ID']}/{job_id}/running"
            urlopen(url, timeout=15).close()
        event = run_job(job)
        exchange = os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-exchange")
        routing = os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation")
    except Exception as exc:  # noqa: BLE001 - Every failed job must publish a failure before acknowledgement.
        if not isinstance(job_id, str) or not job_id:
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        event = {"jobId": job_id, "errorMessage": f"TraitTrail job failed ({type(exc).__name__}); check service logs"}
        exchange, routing = "mas-annotation-failed-exchange", "mas-annotation-failed"
    # Publish confirmation precedes acknowledgement: a lost result leaves the job retryable.
    import pika

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing,
        body=json.dumps(event).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
        mandatory=True,
    )
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, help="Run one job locally instead of consuming RabbitMQ")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not os.environ.get("MAS_ID"):
        parser.error("Set MAS_ID to the registered service Handle (local fixture ID for offline testing)")
    if args.job:
        if not args.output:
            parser.error("--job requires --output")
        event = run_job(json.loads(args.job.read_text()))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(event, indent=2) + "\n")
        print(json.dumps({"jobId": event["jobId"], "annotations": len(event["annotations"]), "schema_valid": True}))
        return
    import pika

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ["RABBITMQ_HOST"],
            credentials=pika.PlainCredentials(os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASSWORD"]),
            heartbeat=0,
            blocked_connection_timeout=30,
        )
    )
    channel = connection.channel()
    channel.confirm_delivery()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=os.environ["RABBITMQ_QUEUE"], on_message_callback=process_message, auto_ack=False)
    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
