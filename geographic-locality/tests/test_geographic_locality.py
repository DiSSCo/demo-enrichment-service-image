"""Tests for the Geographic Locality MAS (community comments: Sámi context, whom to contact)."""

import hashlib
import json
import os
import re
from pathlib import Path

import main
import pytest
import value_service
from conftest import MADE_UP_INDIGENOUS_AND_KVEN, SPECIMEN_ID, FakeChannel, specimen

import shared

REVIEW = "Review status: not reviewed by Sámi representatives."
LOCATION = "$['ods:hasEvents'][1]['ods:hasLocation']"
VENDORED = Path(__file__).resolve().parents[1] / "geographic_locality_mas"


def event_for(spec):
    return main.build_annotation_event(spec, "job-1")


def only_annotation(spec):
    annotations = event_for(spec)["annotations"]
    assert len(annotations) == 1
    return annotations[0]


def lines_of(annotation):
    return annotation["oa:hasBody"]["oa:value"]


def assert_no_made_up_names(data):
    dumped = json.dumps(data, ensure_ascii=False)
    for name in MADE_UP_INDIGENOUS_AND_KVEN:
        assert name not in dumped


def test_register_evidence_gives_a_community_comment(ssr):
    a = only_annotation(specimen())
    assert a["oa:motivation"] == "oa:commenting"
    assert a["oa:hasTarget"]["oa:hasSelector"] == {"@type": "ods:ClassSelector", "ods:class": LOCATION}
    assert a["oa:hasTarget"]["dcterms:identifier"] == SPECIMEN_ID
    lines = lines_of(a)
    assert lines[0] == (
        "A Sámi community may be connected to this locality. Before Sámi knowledge is added to this "
        "record, the community needs to be contacted for collaboration and approval."
    )
    assert (
        "Official name register (Kartverket SSR): a place named in the locality text has "
        "Sámi-language names (sme; 2 within 2000 m). Names are not copied here." in lines
    )
    i = lines.index(
        "The holding institution, Synthetic Herbarium <https://ror.org/000000000>, should contact, "
        "for collaboration and approval, in this order:"
    )
    assert lines[i + 1].startswith("1. Sámi Parliament of Norway <https://sametinget.no/>")
    assert lines[i + 2].startswith("2. Local Sámi community (to be designated by the Sámi side)")
    assert lines[-2:] == ["Built without Sámi input.", REVIEW]
    assert_no_made_up_names(a)


def test_kven_is_never_counted(ssr):
    text = " ".join(lines_of(only_annotation(specimen())))
    assert "Kven" not in text and "fkv" not in text


def test_area_without_coordinates_needs_no_lookup(ssr):
    a = only_annotation(specimen(coords=None, municipality="Karasjok"))
    assert any(line.startswith("Within: Administrative area for Sámi languages, Norway") for line in lines_of(a))
    assert a["oa:hasBody"]["dcterms:references"].startswith("https://www.regjeringen.no/")
    assert ssr.calls == []


def test_no_known_context(ssr):
    ssr.names = [n for n in ssr.names if n["language"] == "norsk"]
    lines = lines_of(only_annotation(specimen(locality="Fjellet")))
    assert lines[0].startswith("No Sámi area and no registered Sámi-language name found.")
    assert lines[-1] == REVIEW


def test_country_not_covered(ssr):
    lines = lines_of(only_annotation(specimen(country="Sweden", coords=(67.85, 20.22))))
    assert "covered: Norway, Finland" in lines[0]
    assert ssr.calls == []


def test_finland_without_key_skips_the_register(ssr):
    lines = lines_of(only_annotation(specimen(country="Finland", coords=(66.5, 25.7), county="Rovaniemi")))
    assert lines[0].startswith("Sámi context not assessed")
    assert ssr.calls == []


def test_specimen_without_location(ssr):
    spec = specimen()
    spec["ods:hasEvents"] = [{"@type": "ods:Event"}]
    a = only_annotation(spec)
    assert a["oa:hasTarget"]["oa:hasSelector"]["ods:class"] == "$['ods:hasEvents']"
    assert lines_of(a)[-1] == REVIEW


def test_names_guard_fails_the_job_without_repeating_the_name(ssr, monkeypatch):
    monkeypatch.setattr(shared, "mark_job_as_running", lambda job_id: None)
    monkeypatch.setattr(  # a contact registry row that holds a register name
        "geographic_locality_mas.community_context.load_contacts",
        lambda path=None: [
            {
                "country": "NO",
                "people": "Sámi",
                "level": "national",
                "contact": "Synth-0001 office",
                "role": None,
                "url": None,
                "status": "test",
            }
        ],
    )
    channel = FakeChannel()
    main.process_message(channel, None, None, json.dumps({"jobId": "job-9", "object": specimen()}).encode())
    [sent] = channel.published
    assert sent["routing_key"] == "mas-annotation-failed"
    assert "Synth-0001" not in sent["body"].decode()


def test_names_the_record_itself_uses_are_allowed(ssr):
    spec = specimen()
    spec["ods:organisationName"] = "Synth-0001 Museum"  # the record's own text
    assert "1. Sámi Parliament" in " ".join(lines_of(only_annotation(spec)))


def test_process_message_publishes_the_event(ssr, monkeypatch):
    monkeypatch.setattr(shared, "mark_job_as_running", lambda job_id: None)
    channel = FakeChannel()
    body = json.dumps({"jobId": "job-7", "object": specimen()}).encode("utf-8")
    main.process_message(channel, None, None, body)
    [sent] = channel.published
    assert (sent["exchange"], sent["routing_key"]) == ("mas-annotation-exchange", "mas-annotation")
    event = json.loads(sent["body"])
    assert event["jobId"] == "job-7" and len(event["annotations"]) == 1
    assert_no_made_up_names(event)


def test_failed_lookup_without_an_area_marks_the_job_failed(ssr, monkeypatch):
    monkeypatch.setattr(shared, "mark_job_as_running", lambda job_id: None)
    ssr.error = "HTTP 503 from https://ws.geonorge.no/stedsnavn/v1/punkt"
    channel = FakeChannel()
    main.process_message(channel, None, None, json.dumps({"jobId": "job-8", "object": specimen()}).encode())
    [sent] = channel.published
    assert (sent["exchange"], sent["routing_key"]) == ("mas-annotation-failed-exchange", "mas-annotation-failed")
    assert json.loads(sent["body"])["jobId"] == "job-8"


def test_failed_lookup_inside_an_area_still_comments(ssr):
    ssr.error = "HTTP 503 from https://ws.geonorge.no/stedsnavn/v1/punkt"
    lines = lines_of(only_annotation(specimen(municipality="Karasjok")))
    assert lines[0].startswith("A Sámi community may be connected to this locality.")


def test_hosted_value_service_is_used_when_configured(ssr, monkeypatch):
    calls = []
    embedded = value_service.enrich_embedded(specimen())

    class Response:
        ok, status_code = True, 200

        def json(self):
            return embedded

    def fake_post(url, json, headers, timeout):
        calls.append((url, json["enrichments"], headers))
        return Response()

    monkeypatch.setenv("VALUE_SERVICE_URL", "https://values.example.org/")
    monkeypatch.setenv("VALUE_SERVICE_API_KEY", "test-key-not-real")
    monkeypatch.setattr(value_service.requests, "post", fake_post)
    a = only_annotation(specimen())
    assert calls == [
        ("https://values.example.org/v1/enrich", ["community_context"], {"Authorization": "Bearer test-key-not-real"})
    ]
    assert lines_of(a)[-1] == REVIEW


def test_vendored_team_code_matches_its_manifest():
    manifest = (VENDORED / "VENDORED.md").read_text(encoding="utf-8")
    rows = re.findall(r"^\| `([^`]+)` \| `([0-9a-f]{64})` \|$", manifest, flags=re.MULTILINE)
    assert len(rows) == 4
    for name, digest in rows:
        assert hashlib.sha256((VENDORED / name).read_bytes()).hexdigest() == digest, name


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
    variants = (specimen(), specimen(locality="Fjellet"), specimen(coords=None, municipality="Karasjok"))
    for spec_variant in variants:
        event = event_for(spec_variant)
        errors = list(Draft202012Validator(event_schema, registry=registry).iter_errors(event))
        assert errors == [], errors
