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


def build_creator() -> dict:
    """Use the registered MAS identity, or an explicit local demo identity."""
    mas_id = os.environ.get("MAS_ID")
    identifier = f"https://hdl.handle.net/{mas_id}" if mas_id else "https://example.org/mas/bdq-validator"
    return {
        "@id": identifier,
        "@type": "schema:SoftwareApplication",
        "schema:identifier": identifier,
        "schema:name": os.environ.get("MAS_NAME", "BDQ Validator"),
        "ods:hasRoles": [{"@type": "schema:Role", "schema:roleName": "machine-annotation-service"}],
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
        "@id": specimen.get("@id", specimen["dcterms:identifier"]),
        "@type": "ods:DigitalSpecimen",
        "dcterms:identifier": specimen["dcterms:identifier"],
        "ods:fdoType": specimen["ods:fdoType"],
    }
    if selector is not None:
        target["oa:hasSelector"] = selector
    return {
        "@type": "ods:Annotation",
        "oa:motivation": "oa:assessing",
        "oa:motivatedBy": f"{test_label}: {response['Response.comment']}",
        "dcterms:created": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "dcterms:creator": build_creator(),
        "oa:hasTarget": target,
        "oa:hasBody": {
            "@type": "oa:TextualBody",
            # DiSSCo's 0.4 body schema requires an array of strings.
            "oa:value": [json.dumps(bdq_result, ensure_ascii=False)],
            "dcterms:references": test_version_iri,
        },
    }


def test_bdq_event(
    job: dict,
    tests: Sequence[BDQEvent | type[BDQEvent]] | None = None,
) -> dict:
    """Assess each event independently and return the original jobId unchanged."""
    if not isinstance(job, dict) or not isinstance(job.get("jobId"), str) or not job["jobId"].strip():
        raise ValueError("The job must contain a nonempty string jobId")
    specimen = job.get("object")
    if not isinstance(specimen, dict):
        raise TypeError("The job object must be an OpenDS Digital Specimen")
    if specimen.get("@type") != "ods:DigitalSpecimen" or specimen.get("ods:fdoType") != DIGITAL_SPECIMEN_FDO_TYPE:
        raise ValueError("This MAS supports Digital Specimens only")
    identifier = specimen.get("dcterms:identifier")
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValueError("The specimen must contain a nonempty dcterms:identifier")
    if "@id" in specimen and (not isinstance(specimen["@id"], str) or not specimen["@id"].strip()):
        raise ValueError("The specimen @id must be a nonempty string")

    events = specimen.get("ods:hasEvents", [])
    if not isinstance(events, list):
        raise TypeError("ods:hasEvents must be an array")

    test_instances: list[BDQEvent] = []
    if tests is not None:
        for t in tests:
            test_instances.append(t() if isinstance(t, type) else t)
    else:
        test_instances = [cls() for cls in BDQEvent.get_registered_tests()]

    # Single-term event tests use the per-event loop below; tests in other OpenDS
    # classes, or reading several terms, are assessed by assess_scope afterwards.
    scoped_tests = [
        test for test in test_instances if getattr(test, "scope", "event") != "event" or getattr(test, "terms", None)
    ]
    test_instances = [test for test in test_instances if not any(test is scoped for scoped in scoped_tests)]

    if not events:
        # No event class exists to select: assess the whole specimen.
        annotations = [
            build_mas_annotation(
                specimen,
                None,
                f"$['ods:hasEvents'][*]['{getattr(test, 'term', 'dwc:eventDate')}']",
                None,
                test=test,
            )
            for test in test_instances
        ]
    else:
        annotations = []
        for index, event in enumerate(events):
            if not isinstance(event, dict):
                raise TypeError("Each ods:hasEvents entry must be an object")
            event_path = f"$['ods:hasEvents'][{index}]"
            for test in test_instances:
                term = getattr(test, "term", None) or getattr(test, "TERM", "dwc:eventDate")
                input_path = f"{event_path}['{term}']"
                if term in event:
                    selector = {"@type": "ods:TermSelector", "ods:term": input_path}
                else:
                    # An assessment cannot select a term absent from the specimen.
                    selector = {"@type": "ods:ClassSelector", "ods:class": event_path}
                annotations.append(build_mas_annotation(specimen, event.get(term), input_path, selector, test=test))
    for test in scoped_tests:
        annotations.extend(assess_scope(specimen, test))
    return {"jobId": job["jobId"], "annotations": annotations}


# Steps from the specimen root to the OpenDS class holding a scope's terms.
# A step ending in [] is an array: the test runs once per entry.
SCOPE_PATHS = {
    "event": ("ods:hasEvents[]",),
    "location": ("ods:hasEvents[]", "ods:hasLocation"),
    "georeference": ("ods:hasEvents[]", "ods:hasLocation", "ods:hasGeoreference"),
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
            return {"@type": "ods:ClassSelector", "ods:class": json_path(keys[:end])}
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
                selector = {"@type": "ods:TermSelector", "ods:term": input_path}
            else:
                # An assessment cannot select a term absent from the specimen.
                selector = nearest_class_selector(specimen, keys)
        annotations.append(build_mas_annotation(specimen, value, input_path, selector, test=test))
    return annotations
