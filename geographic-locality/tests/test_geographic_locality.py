"""Tests for the Geographic Locality MAS (pointer-only Indigenous place-name comments)."""

import json
import os
from pathlib import Path

import main
import pointer_annotations as pa
import pytest
import value_service
from conftest import MADE_UP_INDIGENOUS_AND_KVEN, SPECIMEN_ID, FakeChannel, specimen

import shared

REVIEW = "Review status: not reviewed by Sámi representatives."
LOCATION = "$['ods:hasEvents'][1]['ods:hasLocation']"


def event_for(spec):
    return main.build_annotation_event(spec, "job-1")


def only_annotation(spec):
    """Return the Indigenous place-name annotation, excluding other MAS components."""
    annotations = event_for(spec)["annotations"]
    pointers = [
        annotation for annotation in annotations
        if annotation["oa:hasBody"]["oa:value"][0].startswith(
            "Indigenous place-name pointer (prototype)."
        )
    ]
    assert len(pointers) == 1
    return pointers[0]


def text_of(annotation):
    return annotation["oa:hasBody"]["oa:value"][0]


def assert_no_made_up_names(annotation):
    dumped = json.dumps(annotation, ensure_ascii=False)
    for name in MADE_UP_INDIGENOUS_AND_KVEN:
        assert name not in dumped


def test_match_is_a_pointer_comment_without_names(ssr):
    a = only_annotation(specimen())
    assert a["oa:motivation"] == "oa:commenting"
    assert a["oa:hasTarget"]["oa:hasSelector"] == {"@type": "ods:ClassSelector", "ods:class": LOCATION}
    assert a["oa:hasTarget"]["dcterms:identifier"] == SPECIMEN_ID
    assert a["oa:hasBody"]["dcterms:references"] == "https://stadnamn.kartverket.no/fakta/900001"
    assert a["oa:hasBody"]["ods:score"] == 0.9
    text = text_of(a)
    assert "https://stadnamn.kartverket.no/fakta/900001 (Northern Sámi, sme, hovednavn; confidence 0.9)" in text
    assert "copies no names" in text
    assert "Built without Sámi input" in text
    assert "© Kartverket" in text
    assert text.endswith(REVIEW)
    assert_no_made_up_names(a)


def test_kven_is_never_annotated(ssr):
    text = text_of(only_annotation(specimen()))
    assert "Kven" not in text
    assert "fkv" not in text


def test_nearby_lists_register_entries_only(ssr):
    a = only_annotation(specimen(locality="Fjellet ved elva"))
    text = text_of(a)
    assert "proximity only" in text
    assert "https://stadnamn.kartverket.no/fakta/900001 (Northern Sámi, sme, hovednavn; 300 m" in text
    assert "https://stadnamn.kartverket.no/fakta/900002 (Lule Sámi, smj, hovednavn; 1200 m" in text
    assert a["oa:hasBody"]["ods:score"] == 0.43
    assert_no_made_up_names(a)


def test_no_match(ssr):
    ssr.names = [n for n in ssr.names if n["language"] == "norsk"]
    a = only_annotation(specimen(locality="Fjellet"))
    assert "No Indigenous-language names in Kartverket SSR within 2000 m" in text_of(a)
    assert "ods:score" not in a["oa:hasBody"]
    assert text_of(a).endswith(REVIEW)


def test_no_coordinates(ssr):
    a = only_annotation(specimen(lat=None))
    assert "No decimal coordinates" in text_of(a)
    assert ssr.calls == []


def test_country_not_covered(ssr):
    a = only_annotation(specimen(country="Sweden"))
    assert "covered: Norway, Finland" in text_of(a)
    assert ssr.calls == []


def test_finland_without_key_is_not_configured(ssr):
    a = only_annotation(specimen(country="Finland"))
    assert value_service.NOT_CONFIGURED in text_of(a)
    assert ssr.calls == []


def test_specimen_without_location(ssr):
    spec = specimen()
    spec["ods:hasEvents"] = [{"@type": "ods:Event"}]
    a = only_annotation(spec)
    assert a["oa:hasTarget"]["oa:hasSelector"]["ods:class"] == "$['ods:hasEvents']"
    assert text_of(a).endswith(REVIEW)


def test_guard_refuses_a_name_and_does_not_repeat_it(ssr):
    spec = specimen()
    response = value_service.enrich(spec)
    annotations = pa.build_annotations(spec, response, shared.get_agent(), "2026-10-14T12:00:00.000Z")
    annotations[0]["oa:hasBody"]["oa:value"][0] += " Synth-0001"
    with pytest.raises(pa.NamesInAnnotationError) as err:
        pa.assert_no_names(annotations, response)
    assert "Synth-0001" not in str(err.value)


def test_guard_ignores_majority_language_names(ssr):
    spec = specimen()
    response = value_service.enrich(spec)
    annotations = pa.build_annotations(spec, response, shared.get_agent(), "2026-10-14T12:00:00.000Z")
    annotations[0]["oa:hasBody"]["oa:value"][0] += " Synth-1000"
    pa.assert_no_names(annotations, response)


def test_process_message_publishes_the_event(ssr, monkeypatch):
    monkeypatch.setattr(shared, "mark_job_as_running", lambda job_id: None)
    channel = FakeChannel()
    body = json.dumps({"jobId": "job-7", "object": specimen()}).encode("utf-8")
    main.process_message(channel, None, None, body)
    [sent] = channel.published
    assert (sent["exchange"], sent["routing_key"]) == ("mas-annotation-exchange", "mas-annotation")
    event = json.loads(sent["body"])
    assert event["jobId"] == "job-7"
    assert len(event["annotations"]) == 2
    assert any(
        "Natural Earth country-boundary validation" in a["oa:hasBody"]["oa:value"][0]
        for a in event["annotations"]
    )
    assert_no_made_up_names(event)


def test_failed_lookup_marks_the_job_failed(ssr, monkeypatch):
    monkeypatch.setattr(shared, "mark_job_as_running", lambda job_id: None)
    ssr.error = "HTTP 503 from https://ws.geonorge.no/stedsnavn/v1/punkt"
    channel = FakeChannel()
    main.process_message(channel, None, None, json.dumps({"jobId": "job-8", "object": specimen()}).encode())
    [sent] = channel.published
    assert (sent["exchange"], sent["routing_key"]) == ("mas-annotation-failed-exchange", "mas-annotation-failed")
    assert json.loads(sent["body"])["jobId"] == "job-8"


def test_hosted_value_service_is_used_when_configured(ssr, monkeypatch):
    calls = []
    embedded = value_service.enrich_embedded(specimen())

    class Response:
        ok, status_code = True, 200

        def json(self):
            return embedded

    def fake_post(url, json, headers, timeout):
        calls.append((url, headers))
        return Response()

    monkeypatch.setenv("VALUE_SERVICE_URL", "https://values.example.org/")
    monkeypatch.setenv("VALUE_SERVICE_API_KEY", "test-key-not-real")
    monkeypatch.setattr(value_service.requests, "post", fake_post)
    a = only_annotation(specimen())
    assert calls == [("https://values.example.org/v1/enrich", {"Authorization": "Bearer test-key-not-real"})]
    assert a["oa:hasBody"]["dcterms:references"] == "https://stadnamn.kartverket.no/fakta/900001"


@pytest.mark.skipif(not os.environ.get("OPENDS_DIR"), reason="set OPENDS_DIR to a DiSSCo/openDS checkout")
def test_event_validates_against_opends(ssr):
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource

    model = Path(os.environ["OPENDS_DIR"]) / "data-model"
    resources = []
    for path in model.glob("*/*/0.4.0/schema/*.json"):
        schema = json.loads(path.read_text(encoding="utf-8"))
        resource = Resource.from_contents(schema)
        relative = path.relative_to(model).parts  # kind/name/version/schema/file
        for kind in ("developer-schema", "fdo-type"):
            uri = f"https://schemas.dissco.tech/schemas/{kind}/{relative[1]}/{relative[2]}/{relative[4]}"
            resources.append((uri, resource))
    registry = Registry().with_resources(resources)
    event_schema = json.loads(
        (model / "developer-schema/annotation/0.4.0/schema/annotation-processing-event.json").read_text()
    )
    spec = specimen()
    for spec_variant in (spec, specimen(locality="Fjellet"), specimen(lat=None)):
        event = event_for(spec_variant)
        errors = list(Draft202012Validator(event_schema, registry=registry).iter_errors(event))
        assert errors == [], errors
