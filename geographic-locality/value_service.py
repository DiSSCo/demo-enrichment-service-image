"""Value service of the Geographic Locality MAS: embedded by default, API if configured.

For each location of a Digital Specimen: may a Sámi community be connected to this
locality, and whom should the holding institution contact (community context)? The team
module is embedded (geographic_locality_mas/, see VENDORED.md), so DiSSCo runs everything
and no partner hosting is needed. If VALUE_SERVICE_URL is set, the MAS calls the hosted
value service (POST /v1/enrich) instead. The response has the same shape either way
(docs/value-service-contract.md in the team repo).

Names guard: assess_location() raises if a comment would hold a name from the Sámi or Kven
name registers (job FAILED); the hosted service answers HTTP 500. No coordinate check runs
here yet: qc_status is None ("not checked") until the team's check is integrated.

Environment:
  VALUE_SERVICE_URL      optional, base URL of the hosted value service
  VALUE_SERVICE_API_KEY  optional, sent as 'Authorization: Bearer <key>'
  MML_API_KEY            optional secret: name-register evidence for Finnish locations.
                         Without it, Finnish registers are not checked (the areas still are).
"""

import os
from datetime import UTC, datetime
from typing import Any

import requests
from geographic_locality_mas.community_context import REVIEW_STATUS, assess_location, loc_num

SERVICE = "geographic-locality-value-service (embedded)"
API_TIMEOUT_S = 120


class ValueServiceError(RuntimeError):
    """The hosted value service did not answer: the job is marked FAILED."""


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
                "organisation": attrs.get("ods:organisationName"),
                "organisation_id": attrs.get("ods:organisationID"),
                "country": loc.get("dwc:country"),
                "country_code": loc.get("dwc:countryCode"),
                "state_province": loc.get("dwc:stateProvince"),
                "county": loc.get("dwc:county"),
                "municipality": loc.get("dwc:municipality"),
                "higher_geography": loc.get("dwc:higherGeography"),
                "locality": loc.get("dwc:locality"),
                "verbatim_locality": loc.get("dwc:verbatimLocality"),
                "latitude": loc_num(geo.get("dwc:decimalLatitude")),
                "longitude": loc_num(geo.get("dwc:decimalLongitude")),
                "coordinate_uncertainty_m": loc_num(geo.get("dwc:coordinateUncertaintyInMeters")),
            }
        )
    return specimen_id, out


def enrich_embedded(specimen: dict[str, Any]) -> dict[str, Any]:
    """POST /v1/enrich, run in-process."""
    specimen_id, locations = locations_from_specimen(specimen)
    results = [
        {
            "event_index": loc["event_index"],
            "target": location_path(loc["event_index"]),
            "location": loc,
            "community_context": assess_location(loc, qc_status=None),
        }
        for loc in locations
    ]
    return {
        "service": SERVICE,
        "specimen_id": specimen_id,
        "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "review_status": REVIEW_STATUS,
        "results": results,
        "message": None if results else "No location in the specimen record.",
    }


def enrich_api(specimen: dict[str, Any], url: str, api_key: str | None = None) -> dict[str, Any]:
    """POST /v1/enrich on the hosted value service."""
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    response = requests.post(
        f"{url.rstrip('/')}/v1/enrich",
        json={"specimen": specimen, "enrichments": ["community_context"]},
        headers=headers,
        timeout=API_TIMEOUT_S,
    )
    if not response.ok:
        raise ValueServiceError(f"Value service returned HTTP {response.status_code}")
    return response.json()


def enrich(specimen: dict[str, Any]) -> dict[str, Any]:
    """The value-service response for one Digital Specimen."""
    url = os.environ.get("VALUE_SERVICE_URL")
    if url:
        return enrich_api(specimen, url, os.environ.get("VALUE_SERVICE_API_KEY"))
    return enrich_embedded(specimen)
