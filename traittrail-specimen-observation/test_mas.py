import json
import sys
from types import SimpleNamespace

import main as worker
import pytest
from annotations import MEDIA_FDO, map_event, validate_event
from contract import FEATURES
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
    with pytest.raises(RuntimeError):
        worker.process_message(failed, SimpleNamespace(delivery_tag=1), None, json.dumps(job).encode())
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
