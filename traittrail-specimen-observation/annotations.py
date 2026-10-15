"""Map validated predictions to conservative openDS media comments."""

import datetime as dt
import hashlib
import json
from pathlib import Path

from contract import FEATURES, VERSION, validate
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

SCHEMAS = Path(__file__).with_name("schemas")
MEDIA_FDO = "https://doi.org/21.T11148/bbad8c4e101e8af01115"


def validate_event(event: dict) -> None:
    index = json.loads((SCHEMAS / "index.json").read_text())
    resources = []
    for uri, item in index["schemas"].items():
        data = (SCHEMAS / item["file"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ValueError("Pinned schema checksum mismatch")
        resources.append((uri, Resource.from_contents(json.loads(data))))
    registry = Registry().with_resources(resources)
    schema = registry.contents(index["entry"])
    Draft202012Validator(schema, registry=registry, format_checker=FormatChecker()).validate(event)


def map_event(job: dict, response: dict, mas_id: str, mas_name: str) -> dict:
    media = job["object"]
    if media.get("ods:fdoType") != MEDIA_FDO:
        raise ValueError("This MAS accepts digital media jobs only")
    target_id = media.get("@id")
    if not isinstance(target_id, str) or not target_id.startswith("https://"):
        raise ValueError("Media job needs its real HTTPS persistent identifier")
    if response.get("state") != "usable":
        raise ValueError("Inference did not produce usable observations")
    errors = validate(response.get("observation"), [response.get("image_id")])
    if errors:
        raise ValueError("Invalid observation response: " + "; ".join(errors))
    creator_uri = mas_id if mas_id.startswith("https://") else "https://hdl.handle.net/" + mas_id
    creator = {
        "@id": creator_uri,
        "@type": "schema:SoftwareApplication",
        "schema:identifier": creator_uri,
        "schema:name": mas_name,
    }
    timestamp = dt.datetime.now(dt.UTC).isoformat()
    annotations = []
    for observation in response["observation"]["observations"]:
        # One annotation for each evidence region; unresolved features target the whole media.
        regions = observation["evidence"] or [None]
        for region in regions:
            target = {
                "@id": target_id,
                "@type": "ods:DigitalMediaObject",
                "dcterms:identifier": media.get("dcterms:identifier", target_id),
                "ods:fdoType": MEDIA_FDO,
            }
            if region:
                x, y, width, height = region["bbox"]
                target["oa:hasSelector"] = {
                    "@type": "oa:FragmentSelector",
                    "dcterms:conformsTo": "https://ac.tdwg.org/termlist/#711-region-of-interest-vocabulary",
                    "ac:hasROI": {"ac:xFrac": x, "ac:yFrac": y, "ac:widthFrac": width, "ac:heightFrac": height},
                }
            value = {
                "contract": VERSION,
                "feature_id": observation["feature_id"],
                "feature_label": FEATURES[observation["feature_id"]]["label"],
                "observation_id": response["run_id"] + ":" + observation["feature_id"],
                "status": observation["status"],
                "value": observation["value"],
                "visibility": observation["visibility"],
                "note": observation["note"],
                "model": response["model"],
                "run_id": response["run_id"],
                "request_id": response["request_id"],
                "human_reviewed": False,
                "evidence": observation["evidence"],
                "selected_evidence": region,
                "confidence": observation["confidence"],
                "confidence_note": "Model self-report; uncalibrated",
            }
            annotations.append(
                {
                    "@type": "ods:Annotation",
                    "oa:motivation": "oa:commenting",
                    "oa:motivatedBy": "Visible specimen features predicted from this media; human review required",
                    "dcterms:creator": creator,
                    "dcterms:created": timestamp,
                    "oa:hasTarget": target,
                    "oa:hasBody": {
                        "@type": "oa:TextualBody",
                        "oa:value": [json.dumps(value)],
                        "dcterms:references": media["ac:accessURI"],
                    },
                }
            )
    event = {"jobId": job["jobId"], "annotations": annotations}
    validate_event(event)
    return event
