"""Map OpenDS fields and BDQ responses to DiSSCo annotation events."""

import json
import os
from collections.abc import Sequence
from datetime import UTC, datetime

from bdq import (
    IMPLEMENTATION_VERSION,
    TEST_LABEL,
    TEST_VERSION_IRI,
    BDQEvent,
    ValidationEventDateNotEmpty,
)

DIGITAL_SPECIMEN_FDO_TYPE = "https://doi.org/21.T11148/894b1e6cad57e921764e"

# Shared protocol terms and assessment messages.
EVENT_SCOPE_STEP = "ods:hasEvents[]"
JSONLD_TYPE = "@type"
SPECIMEN_FDO_TYPE = "ods:fdoType"
SPECIMEN_IDENTIFIER = "dcterms:identifier"


def build_creator() -> dict:
    """Use the registered MAS identity, or an explicit local demo identity."""
    mas_id = os.environ.get("MAS_ID")
    identifier = f"https://hdl.handle.net/{mas_id}" if mas_id else "https://example.org/mas/bdq-validator"
    return {
        "@id": identifier,
        JSONLD_TYPE: "schema:SoftwareApplication",
        "schema:identifier": identifier,
        "schema:name": os.environ.get("MAS_NAME", "BDQ Validator"),
        "ods:hasRoles": [{JSONLD_TYPE: "schema:Role", "schema:roleName": "machine-annotation-service"}],
    }


def build_mas_annotation(
    specimen: dict,
    value: str | None,
    input_path: str,
    selector: dict | None,
    test: BDQEvent | type[BDQEvent] | None = None,
) -> dict:
    if test is None:
        test_instance = ValidationEventDateNotEmpty()
    elif isinstance(test, type):
        test_instance = test()
    else:
        test_instance = test

    response = test_instance.validate(value)
    test_label = getattr(test_instance, "test_label", None) or getattr(test_instance, "TEST_LABEL", TEST_LABEL)
    test_version_iri = getattr(test_instance, "test_version_iri", None) or getattr(
        test_instance, "TEST_VERSION_IRI", TEST_VERSION_IRI
    )
    impl_version = getattr(test_instance, "implementation_version", None) or getattr(
        test_instance, "IMPLEMENTATION_VERSION", IMPLEMENTATION_VERSION
    )
    impl_name = getattr(test_instance, "implementation_name", None) or getattr(
        test_instance, "IMPLEMENTATION_NAME", "bdq-validator"
    )
    term = getattr(test_instance, "term", None) or getattr(test_instance, "TERM", "dwc:eventDate")
    # Multi-term tests (with a `terms` attribute) receive and report a {term: value} dict.
    input_values = value if getattr(test_instance, "terms", None) else {term: value}

    bdq_result = {
        "test": test_label,
        "testVersion": test_version_iri,
        "implementation": {"name": impl_name, "version": impl_version},
        "input": input_values,
        "inputPath": input_path,
        **response,
    }
    target = {
        "@id": specimen.get("@id", specimen[SPECIMEN_IDENTIFIER]),
        JSONLD_TYPE: "ods:DigitalSpecimen",
        SPECIMEN_IDENTIFIER: specimen[SPECIMEN_IDENTIFIER],
        SPECIMEN_FDO_TYPE: specimen[SPECIMEN_FDO_TYPE],
    }
    if selector is not None:
        target["oa:hasSelector"] = selector
    return {
        JSONLD_TYPE: "ods:Annotation",
        "oa:motivation": "oa:assessing",
        "oa:motivatedBy": f"{test_label}: {response['Response.comment']}",
        "dcterms:created": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "dcterms:creator": build_creator(),
        "oa:hasTarget": target,
        "oa:hasBody": {
            JSONLD_TYPE: "oa:TextualBody",
            # DiSSCo's 0.4 body schema requires an array of strings.
            "oa:value": [json.dumps(bdq_result, ensure_ascii=False)],
            "dcterms:references": test_version_iri,
        },
    }


def _nonempty_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_job(job: dict) -> dict:
    """Validate the envelope and return its Digital Specimen."""
    if not isinstance(job, dict) or not _nonempty_string(job.get("jobId")):
        raise ValueError("The job must contain a nonempty string jobId")
    specimen = job.get("object")
    if not isinstance(specimen, dict):
        raise TypeError("The job object must be an OpenDS Digital Specimen")
    if (
        specimen.get(JSONLD_TYPE) != "ods:DigitalSpecimen"
        or specimen.get(SPECIMEN_FDO_TYPE) != DIGITAL_SPECIMEN_FDO_TYPE
    ):
        raise ValueError("This MAS supports Digital Specimens only")
    identifier = specimen.get(SPECIMEN_IDENTIFIER)
    if not _nonempty_string(identifier):
        raise ValueError("The specimen must contain a nonempty dcterms:identifier")
    if "@id" in specimen and not _nonempty_string(specimen["@id"]):
        raise ValueError("The specimen @id must be a nonempty string")

    events = specimen.get("ods:hasEvents", [])
    if not isinstance(events, list):
        raise TypeError("ods:hasEvents must be an array")
    return specimen


def _assess_event(specimen: dict, event: dict, index: int, tests: Sequence[BDQEvent]) -> list[dict]:
    if not isinstance(event, dict):
        raise TypeError("Each ods:hasEvents entry must be an object")
    event_path = f"$['ods:hasEvents'][{index}]"
    annotations = []
    for test in tests:
        term = getattr(test, "term", None) or getattr(test, "TERM", "dwc:eventDate")
        input_path = f"{event_path}['{term}']"
        if term in event:
            selector = {JSONLD_TYPE: "ods:TermSelector", "ods:term": input_path}
        else:
            # An assessment cannot select a term absent from the specimen.
            selector = {JSONLD_TYPE: "ods:ClassSelector", "ods:class": event_path}
        annotations.append(build_mas_annotation(specimen, event.get(term), input_path, selector, test=test))
    return annotations


def _assess_events(specimen: dict, tests: Sequence[BDQEvent]) -> list[dict]:
    events = specimen.get("ods:hasEvents", [])
    if not events:
        # No event class exists to select: assess the whole specimen.
        return [
            build_mas_annotation(
                specimen,
                None,
                f"$['ods:hasEvents'][*]['{getattr(test, 'term', 'dwc:eventDate')}']",
                None,
                test=test,
            )
            for test in tests
        ]
    annotations = []
    for index, event in enumerate(events):
        annotations.extend(_assess_event(specimen, event, index, tests))
    return annotations


def test_bdq_event(
    job: dict,
    tests: Sequence[BDQEvent | type[BDQEvent]] | None = None,
) -> dict:
    """Assess each event independently and return the original jobId unchanged."""
    specimen = _validate_job(job)
    selected = tests if tests is not None else BDQEvent.get_registered_tests()
    test_instances = [test() if isinstance(test, type) else test for test in selected]
    event_tests, scoped_tests = [], []
    for test in test_instances:
        if getattr(test, "scope", "event") != "event" or getattr(test, "terms", None):
            scoped_tests.append(test)
        else:
            event_tests.append(test)
    annotations = _assess_events(specimen, event_tests)
    for test in scoped_tests:
        annotations.extend(assess_scope(specimen, test))
    return {"jobId": job["jobId"], "annotations": annotations}


# Steps from the specimen root to the OpenDS class holding a scope's terms.
# A step ending in [] is an array: the test runs once per entry.
SCOPE_PATHS = {
    "event": (EVENT_SCOPE_STEP,),
    "location": (EVENT_SCOPE_STEP, "ods:hasLocation"),
    "georeference": (EVENT_SCOPE_STEP, "ods:hasLocation", "ods:hasGeoreference"),
    "identification": ("ods:hasIdentifications[]",),
    "taxon": ("ods:hasIdentifications[]", "ods:hasTaxonIdentifications[]"),
}


def json_path(keys: Sequence) -> str:
    """("ods:hasEvents", 0, "dwc:eventDate") -> $['ods:hasEvents'][0]['dwc:eventDate']"""
    return "$" + "".join(f"[{key}]" if isinstance(key, int) or key == "*" else f"['{key}']" for key in keys)


def resolve(specimen: dict, keys: Sequence):
    """Follow keys from the specimen root; None when a step is missing."""
    node = specimen
    for key in keys:
        if isinstance(key, int):
            if not isinstance(node, list) or not 0 <= key < len(node):
                return None
        elif not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def scope_contexts(specimen: dict, scope: str):
    """Yield (object, keys) for each OpenDS object holding a scope's terms.

    The object is None when it is absent; keys still give the path it would have.
    """
    if scope == "specimen":
        yield specimen, ()
        return
    if scope not in SCOPE_PATHS:
        raise ValueError(f"Unknown BDQ test scope: {scope}")
    yield from _walk_scope(specimen, (), SCOPE_PATHS[scope])


def _walk_scope(node, keys: tuple, steps: Sequence):
    if not steps:
        yield (node if isinstance(node, dict) else None), keys
        return
    step, rest = steps[0], steps[1:]
    if not step.endswith("[]"):
        yield from _walk_scope(node.get(step) if isinstance(node, dict) else None, (*keys, step), rest)
        return
    collection = step.removesuffix("[]")
    items = node.get(collection, []) if isinstance(node, dict) else []
    if not isinstance(items, list):
        raise TypeError(f"{collection} must be an array")
    if not items:
        # Absent or empty array: one assessment at the path the object would have.
        placeholder = [part for s in rest for part in ((s[:-2], "*") if s.endswith("[]") else (s,))]
        yield None, (*keys, collection, "*", *placeholder)
        return
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise TypeError(f"Each {collection} entry must be an object")
        yield from _walk_scope(item, (*keys, collection, index), rest)


def nearest_class_selector(specimen: dict, keys: Sequence) -> dict | None:
    """Select the deepest existing object on the path, or None for the whole specimen."""
    for end in range(len(keys), 0, -1):
        if isinstance(resolve(specimen, keys[:end]), dict):
            return {JSONLD_TYPE: "ods:ClassSelector", "ods:class": json_path(keys[:end])}
    return None


def assess_scope(specimen: dict, test: BDQEvent) -> list[dict]:
    """Assess a test once per OpenDS object in its scope (see bdq.py for the attributes)."""
    terms = getattr(test, "terms", None)
    consulted_paths = getattr(test, "consulted_paths", {})
    annotations = []
    for found, keys in scope_contexts(specimen, getattr(test, "scope", "event")):
        found = found or {}
        if terms:
            value = {
                term: resolve(specimen, consulted_paths[term]) if term in consulted_paths else found.get(term)
                for term in terms
            }
            input_path = json_path(keys)
            selector = nearest_class_selector(specimen, keys)
        else:
            value = found.get(test.term)
            input_path = json_path((*keys, test.term))
            if test.term in found:
                selector = {JSONLD_TYPE: "ods:TermSelector", "ods:term": input_path}
            else:
                # An assessment cannot select a term absent from the specimen.
                selector = nearest_class_selector(specimen, keys)
        annotations.append(build_mas_annotation(specimen, value, input_path, selector, test=test))
    return annotations
