import json
import logging
import sys
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import main as worker
import pytest
from annotations import MEDIA_FDO, map_event, validate_event
from contract import FEATURES, validate
from jsonschema import ValidationError


@pytest.fixture
def job():
    return {
        "jobId": "test/unchanged",
        "object": {
            "@id": "https://example.org/media/1",
            "ods:fdoType": MEDIA_FDO,
            "ac:accessURI": "https://example.org/image.jpg",
        },
        "batchingRequested": False,
    }


@pytest.fixture
def response():
    return {
        "state": "usable",
        "image_id": "image_test",
        "model": "test-model",
        "run_id": "run_test",
        "request_id": "request_test",
        "observation": {
            "summary": "Unknown",
            "next_view_id": None,
            "next_view_reason": "",
            "observations": [
                {
                    "feature_id": key,
                    "status": "unknown",
                    "value": None,
                    "visibility": "insufficient_detail",
                    "confidence": None,
                    "evidence": [],
                    "note": "Unresolved",
                }
                for key in FEATURES
            ],
        },
    }


def test_region_and_unknown_mapping(job, response):
    observation = next(o for o in response["observation"]["observations"] if o["feature_id"] == "mount.pin_visible")
    observation.update(
        status="observed",
        value=True,
        visibility="clear",
        evidence=[
            {"image_id": "image_test", "bbox": [0.1, 0.2, 0.3, 0.4], "instance_id": None, "description": "Pin region"}
        ],
    )
    event = map_event(job, response, "https://example.org/mas", "Test")
    assert event["jobId"] == "test/unchanged"
    assert len(event["annotations"]) == 16
    region = next(a for a in event["annotations"] if "oa:hasSelector" in a["oa:hasTarget"])
    assert region["oa:hasTarget"]["@id"] == job["object"]["@id"]
    assert region["oa:hasTarget"]["oa:hasSelector"]["ac:hasROI"]["ac:xFrac"] == 0.1
    value = json.loads(region["oa:hasBody"]["oa:value"][0])
    assert not value["human_reviewed"]
    assert value["evidence"] == observation["evidence"]
    assert "ods:score" not in region["oa:hasBody"]
    assert all(a["oa:motivation"] == "oa:commenting" for a in event["annotations"])


def test_invalid_prediction_is_not_published(job, response):
    response["observation"]["observations"][0]["value"] = True
    with pytest.raises(ValueError, match="Invalid observation"):
        map_event(job, response, "https://example.org/mas", "Test")


@pytest.mark.parametrize(
    ("key", "changes", "expected"),
    [
        ("wings.present", {"status": "observed", "value": True}, "require image evidence"),
        ("legs.visible_count", {"status": "observed", "value": 2}, "distinct supported instance IDs"),
        (
            "legs.total_count",
            {"status": "observed", "value": 0, "visibility": "partial"},
            "requires clear relevant anatomy",
        ),
        ("body.pattern", {"status": "observed", "value": "invented"}, "outside vocabulary"),
        ("body.colours", {"status": "observed", "value": ["red", "red"]}, "unique vocabulary colours"),
        ("mount.pin_visible", {"value": True}, "must have null value"),
        ("mount.pin_visible", {"note": " "}, "needs a reason"),
        ("mount.pin_visible", {"confidence": float("nan")}, "non-finite confidence"),
    ],
)
def test_semantic_observation_rejections(response, key, changes, expected):
    observation = next(o for o in response["observation"]["observations"] if o["feature_id"] == key)
    observation.update(changes)
    assert any(expected in error for error in validate(response["observation"], [response["image_id"]]))


@pytest.mark.parametrize(
    ("bbox", "description", "expected"),
    [([0.8, 0.1, 0.4, 0.2], "Region", "within the image"), ([0.1, 0.1, 0.2, 0.2], " ", "requires a description")],
)
def test_invalid_region_evidence_rejected(response, bbox, description, expected):
    response["observation"]["observations"][0]["evidence"] = [
        {"image_id": response["image_id"], "bbox": bbox, "description": description, "instance_id": None}
    ]
    assert any(expected in error for error in validate(response["observation"], [response["image_id"]]))


def test_count_presence_and_total_relationships(response):
    by_id = {o["feature_id"]: o for o in response["observation"]["observations"]}
    evidence = {
        "image_id": response["image_id"],
        "bbox": [0.1, 0.2, 0.3, 0.4],
        "description": "Leg",
        "instance_id": "leg1",
    }
    for key, value in [("legs.present", False), ("legs.visible_count", 1), ("legs.total_count", 0)]:
        by_id[key].update(status="observed", value=value, visibility="clear", evidence=[evidence])
    errors = validate(response["observation"], [response["image_id"]])
    assert any("presence and visible count contradict" in error for error in errors)
    assert "Total legs cannot be fewer than distinct visible legs" in errors


def test_supported_instance_count_accepted(response):
    observation = next(o for o in response["observation"]["observations"] if o["feature_id"] == "legs.visible_count")
    observation.update(
        status="observed",
        value=2,
        visibility="partial",
        evidence=[
            {
                "image_id": response["image_id"],
                "bbox": [x, 0.2, 0.1, 0.2],
                "description": "Visible leg",
                "instance_id": name,
            }
            for x, name in [(0.1, "leg1"), (0.4, "leg2")]
        ],
    )
    assert validate(response["observation"], [response["image_id"]]) == []


def test_full_event_schema_rejects_unknown_fields(job, response):
    event = map_event(job, response, "https://example.org/mas", "Test")
    event["annotations"][0]["api_key"] = "must never appear"
    with pytest.raises(ValidationError):
        validate_event(event)


def test_run_job_preserves_id_and_rejects_batching(job, response, monkeypatch):
    monkeypatch.setenv("MAS_ID", "https://example.org/mas")
    event = worker.run_job(job, image_fetch=lambda url: b"image", inference=response)
    assert event["jobId"] == job["jobId"]
    job["batchingRequested"] = True
    with pytest.raises(ValueError, match="batching"):
        worker.run_job(job, image_fetch=lambda url: pytest.fail("Must not fetch"), inference=response)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org/image",
        "https://user:password@example.org/image",
        "https://127.0.0.1/image",
        "https://[::1]/image",
    ],
)
def test_private_or_credential_media_rejected(url):
    with pytest.raises(ValueError):
        worker.check_media_url(url)


class Channel:
    def __init__(self, fail_publish=False):
        self.actions = []
        self.fail_publish = fail_publish

    def basic_publish(self, **kwargs):
        if self.fail_publish:
            raise RuntimeError("Broker unavailable")
        self.actions.append(("publish", kwargs))

    def basic_ack(self, **kwargs):
        self.actions.append(("ack", kwargs))

    def basic_reject(self, **kwargs):
        self.actions.append(("reject", kwargs))


def test_publish_before_ack_and_failure_route(job, monkeypatch):
    monkeypatch.setitem(sys.modules, "pika", SimpleNamespace(BasicProperties=lambda **kw: kw))
    monkeypatch.delenv("RUNNING_ENDPOINT", raising=False)
    monkeypatch.setattr(worker, "run_job", lambda job: {"jobId": job["jobId"], "annotations": []})
    channel = Channel()
    worker.process_message(channel, SimpleNamespace(delivery_tag=1), None, json.dumps(job).encode())
    assert [a[0] for a in channel.actions] == ["publish", "ack"]
    failed = Channel(fail_publish=True)
    method = SimpleNamespace(delivery_tag=1)
    body = json.dumps(job).encode()
    with pytest.raises(RuntimeError):
        worker.process_message(failed, method, None, body)
    assert not failed.actions

    def raises(job):
        raise RuntimeError("private-secret")

    monkeypatch.setattr(worker, "run_job", raises)
    channel = Channel()
    worker.process_message(channel, SimpleNamespace(delivery_tag=1), None, json.dumps(job).encode())
    sent = channel.actions[0][1]
    assert sent["routing_key"] == "mas-annotation-failed"
    assert b"private-secret" not in sent["body"]
    assert json.loads(sent["body"])["jobId"] == job["jobId"]


def test_malformed_message_rejected():
    channel = Channel()
    worker.process_message(channel, SimpleNamespace(delivery_tag=1), None, b"not-json")
    assert channel.actions == [("reject", {"delivery_tag": 1, "requeue": False})]


def test_job_stage_logs(job, response, monkeypatch, caplog):
    monkeypatch.setenv("MAS_ID", "https://example.org/mas")
    caplog.set_level(logging.INFO, logger=worker.LOGGER.name)
    event = worker.run_job(job, image_fetch=lambda url: b"image", inference=response)
    assert len(event["annotations"]) == 16
    records = [json.loads(record.message) for record in caplog.records if record.name == worker.LOGGER.name]
    assert [record["stage"] for record in records if record["event"] == "stage_completed"] == [
        "validate_job",
        "download_media",
        "map_annotations",
    ]


def test_http_failure_logs_stage_without_secrets(job, response, monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "pika", SimpleNamespace(BasicProperties=lambda **kw: kw))
    monkeypatch.delenv("RUNNING_ENDPOINT", raising=False)
    monkeypatch.setenv("TRAITTRAIL_API_TOKEN", "private-secret")
    caplog.set_level(logging.INFO, logger=worker.LOGGER.name)
    original_run_job = worker.run_job

    def blocked_download(url):
        raise HTTPError("https://example.org/?key=private-secret", 403, "private-secret", {}, None)

    def run_blocked_job(current_job):
        return original_run_job(current_job, image_fetch=blocked_download, inference=response)

    monkeypatch.setattr(worker, "run_job", run_blocked_job)
    channel = Channel()
    worker.process_message(channel, SimpleNamespace(delivery_tag=1), None, json.dumps(job).encode())
    records = [json.loads(record.message) for record in caplog.records if record.name == worker.LOGGER.name]
    failure = next(record for record in records if record["event"] == "stage_failed")
    assert failure["stage"] == "download_media"
    assert failure["http_status"] == 403
    assert failure["frames"]
    assert "private-secret" not in caplog.text
    assert "https://example.org/?key=" not in caplog.text
    assert b"private-secret" not in channel.actions[0][1]["body"]
    assert [action[0] for action in channel.actions] == ["publish", "ack"]


@pytest.mark.parametrize(
    "error",
    [
        HTTPError("https://example.org/?key=private-secret", status, "private-secret", {}, None)
        for status in (401, 404, 503)
    ]
    + [URLError("private-secret"), TimeoutError("private-secret")],
)
def test_running_callback_failure_warns_and_still_publishes_annotations(job, response, monkeypatch, caplog, error):
    monkeypatch.setitem(sys.modules, "pika", SimpleNamespace(BasicProperties=lambda **kw: kw))
    monkeypatch.setenv("RUNNING_ENDPOINT", "https://example.org/running")
    monkeypatch.setenv("MAS_ID", "SANDBOX/test-mas")
    caplog.set_level(logging.INFO, logger=worker.LOGGER.name)

    def denied_running_endpoint(url, **kwargs):
        raise error

    monkeypatch.setattr(worker, "urlopen", denied_running_endpoint)
    original_run_job = worker.run_job
    monkeypatch.setattr(
        worker, "run_job", lambda job: original_run_job(job, image_fetch=lambda url: b"image", inference=response)
    )
    channel = Channel()
    worker.process_message(channel, SimpleNamespace(delivery_tag=1), None, json.dumps(job).encode())
    records = [json.loads(record.message) for record in caplog.records if record.name == worker.LOGGER.name]
    warning = next(record for record in records if record["event"] == "running_notification_failed")
    assert warning["continuing"] is True
    assert warning["error_type"] == type(error).__name__
    if isinstance(error, HTTPError):
        assert warning["http_status"] == error.code
    assert any(record.levelno == logging.WARNING for record in caplog.records)
    assert not any(record["event"] == "job_failed" for record in records)
    assert "private-secret" not in caplog.text
    assert "https://example.org/?key=" not in caplog.text
    assert [action[0] for action in channel.actions] == ["publish", "ack"]
    published = channel.actions[0][1]
    event = json.loads(published["body"])
    assert published["routing_key"] == "mas-annotation"
    assert event["jobId"] == job["jobId"]
    assert len(event["annotations"]) == 16
    validate_event(event)


@pytest.mark.parametrize("mas_id", ["SANDBOX/test-mas", "https://hdl.handle.net/SANDBOX/test-mas"])
def test_running_callback_uses_handle_path_and_preserves_creator_id(mas_id, monkeypatch):
    monkeypatch.setenv("RUNNING_ENDPOINT", "https://example.org/api/mjr/v1/")
    monkeypatch.setenv("MAS_ID", mas_id)
    calls = []

    def capture_callback(url, *, timeout):
        calls.append((url, timeout))
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(worker, "urlopen", capture_callback)
    worker.notify_running("SANDBOX/test-job")
    assert calls == [("https://example.org/api/mjr/v1/SANDBOX/test-mas/SANDBOX/test-job/running", 15)]
    assert worker.os.environ["MAS_ID"] == mas_id


def test_startup_logs_configuration_presence_without_values(monkeypatch, caplog):
    monkeypatch.setattr(sys, "argv", ["main.py"])
    monkeypatch.delenv("MAS_ID", raising=False)
    monkeypatch.setenv("TRAITTRAIL_API_TOKEN", "private-secret")
    caplog.set_level(logging.INFO, logger=worker.LOGGER.name)
    with pytest.raises(SystemExit):
        worker.main()
    records = [json.loads(record.message) for record in caplog.records if record.name == worker.LOGGER.name]
    startup = next(record for record in records if record["event"] == "startup")
    assert startup["configured"]["TRAITTRAIL_API_TOKEN"] is True
    assert startup["configured"]["MAS_ID"] is False
    assert "private-secret" not in caplog.text
