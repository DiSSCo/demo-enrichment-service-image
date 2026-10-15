"""DiSSCo RabbitMQ wrapper and local end-to-end job runner."""

import argparse
import base64
import ipaddress
import json
import logging
import os
import socket
import sys
import time
import traceback
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

from annotations import MEDIA_FDO, map_event

MAX_IMAGE_BYTES = 10 * 1024 * 1024
LOGGER = logging.getLogger("traittrail.mas")


def log_event(level, event, *, job_id=None, **details):
    """Log stage metadata, never request bodies, headers, URLs or exception text."""
    record = {"event": event, **details}
    if isinstance(job_id, str):
        record["job_id"] = job_id[:200]
    LOGGER.log(level, "%s", json.dumps(record))


def error_details(error):
    details = {"error_type": type(error).__name__}
    if isinstance(error, HTTPError):
        details["http_status"] = error.code
    if isinstance(error, URLError):
        details["network_reason_type"] = type(error.reason).__name__
    details["frames"] = [
        {"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
        for frame in traceback.extract_tb(error.__traceback__)[-6:]
    ]
    return details


def run_stage(stage, job_id, operation, *args):
    started = time.monotonic()
    log_event(logging.INFO, "stage_started", job_id=job_id, stage=stage)
    try:
        result = operation(*args)
    except Exception as error:
        log_event(logging.ERROR, "stage_failed", job_id=job_id, stage=stage, **error_details(error))
        raise
    log_event(logging.INFO, "stage_completed", job_id=job_id, stage=stage, seconds=round(time.monotonic() - started, 3))
    return result


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


def checked_media(job):
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
    return media


def infer_image(data):
    body = json.dumps({"image_base64": base64.b64encode(data).decode()}).encode()
    headers = {"Content-Type": "application/json"}
    token = os.environ.get("TRAITTRAIL_API_TOKEN")
    if token:
        headers["Authorization"] = "Bearer " + token
    endpoint = os.environ.get("TRAITTRAIL_API_URL", "http://127.0.0.1:8766").rstrip("/")
    with urlopen(Request(endpoint + "/infer", body, headers), timeout=660) as response:
        return json.load(response)


def run_job(job: dict, *, image_fetch=fetch_image, inference=None) -> dict:
    job_id = job.get("jobId") if isinstance(job, dict) else None
    media = run_stage("validate_job", job_id, checked_media, job)
    data = run_stage("download_media", job_id, image_fetch, media["ac:accessURI"])
    if inference is None:
        inference = run_stage("inference", job_id, infer_image, data)
    return run_stage(
        "map_annotations",
        job_id,
        map_event,
        job,
        inference,
        os.environ["MAS_ID"],
        os.environ.get("MAS_NAME", "TraitTrail specimen observations"),
    )


def notify_running(job_id):
    running = os.environ.get("RUNNING_ENDPOINT")
    if running:
        # Matches current demo repository shared.mark_job_as_running.
        mas_id = os.environ["MAS_ID"].removeprefix("https://hdl.handle.net/")
        url = f"{running.rstrip('/')}/{mas_id}/{job_id}/running"
        try:
            urlopen(url, timeout=15).close()
        except OSError as error:
            # Progress is advisory. Preserve image/model failures as real job failures.
            log_event(
                logging.WARNING, "running_notification_failed", job_id=job_id, continuing=True, **error_details(error)
            )


def process_message(channel, method, properties, body):
    try:
        job = json.loads(body)
        if not isinstance(job, dict):
            raise ValueError("Invalid job")  # noqa: TRY004 - JSON input boundary.
    except ValueError:
        log_event(logging.WARNING, "message_rejected", reason="invalid_json_object")
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return
    job_id = job.get("jobId")
    log_event(logging.INFO, "job_received", job_id=job_id)
    try:
        run_stage("notify_running", job_id, notify_running, job_id)
        event = run_job(job)
        exchange = os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-exchange")
        routing = os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation")
    except Exception as exc:  # noqa: BLE001 - Every failed job must publish a failure before acknowledgement.
        log_event(logging.ERROR, "job_failed", job_id=job_id, **error_details(exc))
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
    log_event(
        logging.INFO, "job_acknowledged", job_id=job_id, outcome="failed" if "errorMessage" in event else "completed"
    )


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    setting_names = (
        "MAS_ID",
        "RABBITMQ_HOST",
        "RABBITMQ_USER",
        "RABBITMQ_PASSWORD",
        "RABBITMQ_QUEUE",
        "RUNNING_ENDPOINT",
        "TRAITTRAIL_API_URL",
        "TRAITTRAIL_API_TOKEN",
    )
    log_event(logging.INFO, "startup", configured={name: bool(os.environ.get(name)) for name in setting_names})
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
    log_event(logging.INFO, "queue_listening")
    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - Keep startup diagnostics available without exposing credentials.
        log_event(logging.ERROR, "startup_failed", **error_details(error))
        raise SystemExit(1) from None
