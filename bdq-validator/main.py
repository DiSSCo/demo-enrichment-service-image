"""Run a BDQ job from a JSON file, the DiSSCo sandbox, or RabbitMQ."""

import argparse
import json
import logging
import os
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from annotations import test_bdq_event
from bdq import ALL_TESTS, IMPLEMENTATION_VERSION, TESTS_BY_LABEL

LOGGER = logging.getLogger(__name__)
SANDBOX_API = "https://sandbox.dissco.tech/api/digital-specimen/v1/"


def select_tests(selection: str | None) -> list:
    """Map "all" or comma-separated test labels to test classes; None or blank means all."""
    if not selection or not selection.strip() or selection.strip().lower() == "all":
        return list(ALL_TESTS)
    labels = [label.strip() for label in selection.split(",") if label.strip()]
    unknown = [label for label in labels if label not in TESTS_BY_LABEL]
    if unknown:
        raise ValueError(f"Unknown BDQ test(s): {', '.join(unknown)}. Available: {', '.join(TESTS_BY_LABEL)}")
    return [TESTS_BY_LABEL[label] for label in labels]


def fetch_sandbox_job(specimen_id: str, job_id: str) -> dict:
    specimen_id = specimen_id.removeprefix("https://sandbox.dissco.tech/ds/")
    if not specimen_id.startswith("SANDBOX/") or len(specimen_id.split("/")) != 2:
        raise ValueError("Use a SANDBOX/XXX-XXX-XXX identifier or its sandbox specimen page URL")
    request = Request(
        SANDBOX_API + quote(specimen_id, safe="/"),
        headers={"Accept": "application/json", "User-Agent": f"bdq-validator/{IMPLEMENTATION_VERSION}"},
    )
    with urlopen(request, timeout=20) as response:
        document = json.load(response)
    try:
        specimen = document["data"]["attributes"]
    except (KeyError, TypeError) as error:
        raise ValueError("The sandbox response does not contain data.attributes") from error
    return {"jobId": job_id, "batchingRequested": False, "object": specimen}


def mark_job_as_running(job_id: str) -> None:
    endpoint = os.environ.get("RUNNING_ENDPOINT")
    if endpoint:
        # Same URL construction as shared.mark_job_as_running in the MAS template.
        request = Request(f"{endpoint.rstrip('/')}/{os.environ['MAS_ID']}/{job_id}/running")
        try:
            with urlopen(request, timeout=15) as response:
                LOGGER.debug("Marked job %s as running (HTTP %s)", job_id, response.status)
        except OSError as error:
            # HTTPError, URLError and socket timeouts are OSError subclasses.
            # Reporting progress is optional; still assess and complete the job.
            LOGGER.warning("Could not mark job %s as running: %s; continuing the assessment", job_id, error)


def publish_event(event: dict, channel, *, failed: bool = False) -> None:
    """Publish with broker confirmation enabled by run_rabbitmq."""
    import pika

    if failed:
        exchange = os.environ.get("RABBITMQ_FAILED_EXCHANGE", "mas-annotation-failed-exchange")
        routing_key = os.environ.get("RABBITMQ_FAILED_ROUTING_KEY", "mas-annotation-failed")
    else:
        exchange = os.environ.get("RABBITMQ_EXCHANGE", "mas-annotation-exchange")
        routing_key = os.environ.get("RABBITMQ_ROUTING_KEY", "mas-annotation")
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        mandatory=True,
    )


def process_message(channel, method, properties, body: bytes) -> None:
    job_id = None
    try:
        job = json.loads(body.decode("utf-8"))
        if isinstance(job, dict) and isinstance(job.get("jobId"), str) and job["jobId"].strip():
            job_id = job["jobId"]
        if job_id is None:
            raise ValueError("The message must contain a nonempty string jobId")
        mark_job_as_running(job_id)
        event = test_bdq_event(job, tests=select_tests(os.environ.get("BDQ_TESTS")))
    except Exception as error:
        LOGGER.exception("BDQ job failed: %s", job_id)
        if job_id is None:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        event = {"jobId": job_id, "errorMessage": str(error)}
        publish_event(event, channel, failed=True)
    else:
        publish_event(event, channel)
    # If publishing fails, this line is never reached. Closing the connection
    # leaves the input job available for redelivery rather than losing it.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def run_rabbitmq() -> None:
    import pika

    required = ["RABBITMQ_HOST", "RABBITMQ_USER", "RABBITMQ_PASSWORD", "RABBITMQ_QUEUE", "MAS_ID"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"Missing RabbitMQ settings: {', '.join(missing)}")
    select_tests(os.environ.get("BDQ_TESTS"))  # Fail at startup, not per job, on unknown labels.
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ["RABBITMQ_HOST"],
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            virtual_host=os.environ.get("RABBITMQ_VHOST", "/"),
            credentials=pika.PlainCredentials(os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASSWORD"]),
            heartbeat=60,
            blocked_connection_timeout=60,
        )
    )
    try:
        channel = connection.channel()
        channel.confirm_delivery()
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=os.environ["RABBITMQ_QUEUE"], on_message_callback=process_message, auto_ack=False)
        LOGGER.info("Waiting for BDQ jobs on %s", os.environ["RABBITMQ_QUEUE"])
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()


def write_json(document: dict, destination: Path) -> None:
    """Write to a file explicitly selected by the local CLI operator.

    Open output files after processing succeeds. RabbitMQ messages never supply
    filenames; the file type handles CLI paths separately from JSON content.
    """
    safe_destination = destination.resolve()
    with argparse.FileType("w", encoding="utf-8")(str(safe_destination)) as stream:
        json.dump(document, stream, indent=2, ensure_ascii=False)
        stream.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=argparse.FileType("r", encoding="utf-8"), help="Read a saved MAS job envelope")
    source.add_argument("--specimen", help="Fetch a live specimen from the DiSSCo sandbox")
    source.add_argument("--rabbitmq", action="store_true", help="Consume jobs and publish annotation events")
    parser.add_argument("--job-id", default="local-bdq-validator", help="Job identifier for a sandbox run")
    parser.add_argument("--output", type=Path, help="Write the annotation event to a file instead of stdout")
    parser.add_argument("--save-input", type=Path, help="Save the fetched sandbox job for offline reruns")
    parser.add_argument(
        "--tests",
        default=os.environ.get("BDQ_TESTS"),
        help='"all" (default) or comma-separated BDQ test labels; also read from env BDQ_TESTS',
    )
    args = parser.parse_args()
    if args.save_input and not args.specimen:
        parser.error("--save-input requires --specimen")
    if args.rabbitmq and args.output:
        parser.error("--output is for local runs")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        tests = select_tests(args.tests)
        if args.rabbitmq:
            if args.tests:
                os.environ["BDQ_TESTS"] = args.tests  # process_message reads it per job.
            run_rabbitmq()
            return
        if args.specimen:
            job = fetch_sandbox_job(args.specimen, args.job_id)
            if args.save_input:
                write_json(job, args.save_input)
        else:
            with args.input as stream:
                job = json.load(stream)
        event = test_bdq_event(job, tests=tests)
        if args.output:
            write_json(event, args.output)
        else:
            print(json.dumps(event, indent=2, ensure_ascii=False))
    except (OSError, ValueError, TypeError, ImportError, argparse.ArgumentTypeError) as error:
        parser.exit(status=1, message=f"bdq-validator: {error}\n")


if __name__ == "__main__":
    main()
