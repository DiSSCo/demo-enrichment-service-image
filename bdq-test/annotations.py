"""Map OpenDS event dates and BDQ responses to DiSSCo annotation events."""

import json
import os
from datetime import UTC, datetime

from bdq import IMPLEMENTATION_VERSION, TEST_LABEL, TEST_VERSION_IRI, validate_event_date_notempty

DIGITAL_SPECIMEN_FDO_TYPE = "https://doi.org/21.T11148/894b1e6cad57e921764e"


def build_creator() -> dict:
    """Use the registered MAS identity, or an explicit local demo identity."""
    mas_id = os.environ.get("MAS_ID")
    identifier = f"https://hdl.handle.net/{mas_id}" if mas_id else "https://example.org/mas/bdq-test"
    return {
        "@id": identifier,
        "@type": "schema:SoftwareApplication",
        "schema:identifier": identifier,
        "schema:name": os.environ.get("MAS_NAME", "BDQ event date test"),
        "ods:hasRoles": [{"@type": "schema:Role", "schema:roleName": "machine-annotation-service"}],
    }


def build_mas_annotation(specimen: dict, value: str | None, input_path: str, selector: dict | None) -> dict:
    response = validate_event_date_notempty(value)
    bdq_result = {
        "test": TEST_LABEL,
        "testVersion": TEST_VERSION_IRI,
        "implementation": {"name": "bdq-test", "version": IMPLEMENTATION_VERSION},
        "input": {"dwc:eventDate": value},
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
        "oa:motivatedBy": f"{TEST_LABEL}: {response['Response.comment']}",
        "dcterms:created": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "dcterms:creator": build_creator(),
        "oa:hasTarget": target,
        "oa:hasBody": {
            "@type": "oa:TextualBody",
            # DiSSCo's 0.4 body schema requires an array of strings.
            "oa:value": [json.dumps(bdq_result, ensure_ascii=False)],
            "dcterms:references": TEST_VERSION_IRI,
        },
    }


def test_bdq_event(job: dict) -> dict:
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
    if not events:
        # No event class exists to select: assess the whole specimen.
        annotations = [build_mas_annotation(specimen, None, "$['ods:hasEvents'][*]['dwc:eventDate']", None)]
    else:
        annotations = []
        for index, event in enumerate(events):
            if not isinstance(event, dict):
                raise TypeError("Each ods:hasEvents entry must be an object")
            event_path = f"$['ods:hasEvents'][{index}]"
            input_path = f"{event_path}['dwc:eventDate']"
            if "dwc:eventDate" in event:
                selector = {"@type": "ods:TermSelector", "ods:term": input_path}
            else:
                # An assessment cannot select a term absent from the specimen.
                selector = {"@type": "ods:ClassSelector", "ods:class": event_path}
            annotations.append(build_mas_annotation(specimen, event.get("dwc:eventDate"), input_path, selector))
    return {"jobId": job["jobId"], "annotations": annotations}
