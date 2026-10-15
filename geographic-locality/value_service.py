"""Value service of the Geographic Locality MAS: embedded by default, API if configured.

The team's value service (https://github.com/craigmateo/dissco-geographic-locality-mas,
POST /v1/enrich) is embedded here, so DiSSCo runs everything and no partner hosting
is needed: indigenous_names.py is an unchanged copy of the team module (see README).
If VALUE_SERVICE_URL is set, the MAS calls the hosted API instead. The response has
the same shape either way (docs/value-service-contract.md in the team repo).

Environment:
  VALUE_SERVICE_URL      optional, base URL of the hosted value service
  VALUE_SERVICE_API_KEY  optional, sent as 'Authorization: Bearer <key>'
  MML_API_KEY            optional secret. Without it, Finnish locations get a
                         "not configured" comment instead of an NLS lookup.

The response holds gazetteer name strings. Never log it or copy it into an
annotation: pointer_annotations.py builds the annotations from links and codes only.
"""

import os
from datetime import UTC, datetime
from typing import Any

import indigenous_names as ina
import requests

SERVICE = "geographic-locality-value-service (embedded)"
API_TIMEOUT_S = 120
NOT_CONFIGURED = "NLS Finland lookups are not configured in this deployment (no MML_API_KEY)."


def location_path(event_index: int) -> str:
    return f"$['ods:hasEvents'][{event_index}]['ods:hasLocation']"


def locations_from_specimen(specimen: dict[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    """specimen_id and one flat location per event that has a location (as service.py).

    Accepts the bare openDS object (RabbitMQ message['object']) and the API wrapper
    {'id', 'attributes'}. event_index is the position in ods:hasEvents.
    """
    attrs = specimen.get("attributes") or specimen
    specimen_id = specimen.get("id") or attrs.get("@id") or attrs.get("dcterms:identifier")
    out = []
    for i, event in enumerate(attrs.get("ods:hasEvents") or []):
        loc = (event or {}).get("ods:hasLocation")
        if not loc:
            continue
        geo = loc.get("ods:hasGeoreference") or {}
        out.append(
            {
                "specimen_id": specimen_id,
                "event_index": i,
                "country": loc.get("dwc:country"),
                "country_code": loc.get("dwc:countryCode"),
                "state_province": loc.get("dwc:stateProvince"),
                "county": loc.get("dwc:county"),
                "municipality": loc.get("dwc:municipality"),
                "locality": loc.get("dwc:locality"),
                "verbatim_locality": loc.get("dwc:verbatimLocality"),
                "latitude": ina.num1(geo.get("dwc:decimalLatitude")),
                "longitude": ina.num1(geo.get("dwc:decimalLongitude")),
                "coordinate_uncertainty_m": ina.num1(geo.get("dwc:coordinateUncertaintyInMeters")),
            }
        )
    return specimen_id, out


def lookup(location: dict[str, Any]) -> dict[str, Any]:
    """find_indigenous_names() for one location; Finland without a key -> not_covered."""
    if ina.gazetteer_for(location) == "nls" and not os.environ.get("MML_API_KEY"):
        return {
            "specimen_id": location.get("specimen_id"),
            "event_index": location.get("event_index", 0),
            "status": "not_covered",
            "gazetteer": ina.GAZETTEERS["nls"]["label"],
            "query": None,
            "matches": [],
            "minority_language_names": [],
            "nearby": [],
            "summary": None,
            "message": NOT_CONFIGURED,
            "retrieved": datetime.now(UTC).date().isoformat(),
        }
    return ina.find_indigenous_names(location)


def enrich_embedded(specimen: dict[str, Any]) -> dict[str, Any]:
    """POST /v1/enrich, run in-process."""
    specimen_id, locations = locations_from_specimen(specimen)
    results = [
        {
            "event_index": loc["event_index"],
            "target": location_path(loc["event_index"]),
            "location": loc,
            "indigenous_names": {**lookup(loc), "review_status": ina.REVIEW_STATUS},
        }
        for loc in locations
    ]
    return {
        "service": SERVICE,
        "specimen_id": specimen_id,
        "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "review_status": ina.REVIEW_STATUS,
        "results": results,
        "message": None if results else "No location in the specimen record.",
    }


def enrich_api(specimen: dict[str, Any], url: str, api_key: str | None = None) -> dict[str, Any]:
    """POST /v1/enrich on the hosted value service."""
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    response = requests.post(
        f"{url.rstrip('/')}/v1/enrich",
        json={"specimen": specimen, "enrichments": ["indigenous_names"]},
        headers=headers,
        timeout=API_TIMEOUT_S,
    )
    if not response.ok:
        raise ina.GazetteerError(f"Value service returned HTTP {response.status_code}")
    return response.json()


def enrich(specimen: dict[str, Any]) -> dict[str, Any]:
    """The value-service response for one Digital Specimen."""
    url = os.environ.get("VALUE_SERVICE_URL")
    if url:
        return enrich_api(specimen, url, os.environ.get("VALUE_SERVICE_API_KEY"))
    return enrich_embedded(specimen)
