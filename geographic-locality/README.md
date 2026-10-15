# Geographic Locality MAS: Indigenous community context, Sámi (prototype)

DiSSCo MAS Hackathon 2026, Geographic place names team (Craig Frayne (lead), Lena Thöle,
Nidia Alvarez Armada, Franck Theeten, Marcus Ernst). Team repository:
https://github.com/craigmateo/dissco-geographic-locality-mas

For each location of a Digital Specimen the MAS asks: **may a Sámi community be connected to
this locality?** If so, it posts **one comment** saying that the community needs to be
contacted for collaboration and approval before Sámi knowledge is added to the record, and
that the holding institution should make the contact (Sámi Parliament, then the local
community). It copies no names and changes nothing in the record.
Concept: `docs/indigenous-community-context.md` in the team repository.

> Built without Sámi input. Every annotation ends with "Built without Sámi input." and
> "Review status: not reviewed by Sámi representatives". The contact registry is a
> placeholder until the Sámi side decides who speaks for whom; every contact says how sure it is.

## Flow

```
RabbitMQ (job + Digital Specimen)
  -> value_service.enrich()        embedded team module (geographic_locality_mas/), or the
                                   hosted API if VALUE_SERVICE_URL is set: one location per
                                   event -> assess_location() -> community context + comment
  -> names guard                   refuses a comment that holds a name from the Sámi or Kven
                                   name registers
  -> community_annotations         one oa:commenting per location
  -> mas-annotation-exchange       (failure exchange + job FAILED on any error)
```

| Community context | Comment on `$['ods:hasEvents'][i]['ods:hasLocation']` |
|---|---|
| `indigenous_context` | a Sámi community may be connected; contact for collaboration and approval; the areas the admin units lie in (with the law that defines them); register evidence as counts and ISO 639-3 codes; whom the holding institution should contact, in order; a candidate Local Contexts BC Notice |
| `no_known_context`, `not_assessed`, `not_covered` | one sentence saying why (covered: Norway, Finland) |
| `error` (name register down, no listed area) | failure message, job FAILED (can be re-run) |
| no location in the record | comment on `$['ods:hasEvents']` |

- Evidence: the locality's admin units (`dwc:municipality`, `dwc:county`, `dwc:stateProvince`, `dwc:higherGeography`, never the locality text) lie in Norway's administrative area for Sámi languages or Finland's Sámi Homeland; or the official name registers (Kartverket SSR; NLS Finland only with `MML_API_KEY`) have Sámi-language names at or near the point. Only counts and language codes are kept. Kven is never counted.
- No coordinate check runs in this MAS yet: coordinates count as "not checked". When the team's check is integrated, `assess_location(location, qc_status)` takes its status.
- Only `oa:commenting`: never merged into the record, and its creator can delete it. A deleted DiSSCo annotation stays readable as a tombstone.

## Geographic coordinate validation (integration in progress)

**Contributor:** Craig Frayne

This component checks whether a Digital Specimen's recorded coordinates are
geographically consistent with its recorded country. It uses Natural Earth
country boundaries and normalizes common country-name variants.

For each location in `ods:hasEvents`, the validator:

1. Extracts the country, latitude and longitude.
2. Checks whether the coordinates fall within the recorded country.
3. Applies a configurable boundary tolerance (default: 5 km).
4. Identifies possible latitude/longitude sign errors.
5. Produces structured findings intended for location-specific
   `ods:commenting` annotations.

Validation statuses:

| Status | Meaning |
|---|---|
| `consistent` | Coordinates fall within the recorded country |
| `near_boundary` | Coordinates fall just outside the boundary, within tolerance |
| `inconsistent` | Coordinates fall outside the recorded country and tolerance |
| `unable_to_validate` | Insufficient information or unknown country |
| `invalid_coordinates` | Invalid coordinate values or ranges |

Suggested coordinate corrections require human review. The MAS does not
automatically modify specimen coordinates.

**Limitations:** Natural Earth represents modern political boundaries.
Historical collecting localities, offshore specimens, disputed territories,
uncertain georeferencing and boundary inaccuracies can produce misleading
results. A geographic inconsistency is a quality-control flag, not proof
that the specimen record is incorrect.

Source implementation:
https://github.com/craigmateo/dissco-geographic-locality-mas

## Environment

None required. DiSSCo injects `RABBITMQ_*`, `MAS_ID`, `MAS_NAME`, `RUNNING_ENDPOINT`.

| Variable | Kind | Use |
|---|---|---|
| `MML_API_KEY` | secret, optional | NLS Finland register evidence. Not set for the demo: Finnish registers are not checked, the areas still are |
| `VALUE_SERVICE_URL` | env, optional | call the hosted value service (`POST /v1/enrich`) instead of the embedded module |
| `VALUE_SERVICE_API_KEY` | secret, optional | bearer key for that API |

## Registration (for DiSSCo)

`registration.json` (validates against openDS 0.4.0 `machine-annotation-service-request`):

- image `public.ecr.aws/dissco/geographic-locality`, batching off, not ingestion-compatible, 1 replica;
- the filter limits the MAS to three Sandbox test specimens (`$['dcterms:identifier']`).

## Run locally (dry run, publishes nothing)

From the repository root, with network access to sandbox.dissco.tech and ws.geonorge.no:

```bash
python3 -m venv venv && venv/bin/pip install -r geographic-locality/requirements.txt
PYTHONPATH=. venv/bin/python geographic-locality/main.py \
  SANDBOX/X16-9NF-V0S SANDBOX/KZJ-R72-SN2 SANDBOX/KSS-ADF-LZD
```

The dry run prints the annotation events (JSON, stdout) exactly as the MAS would publish them.

## Tests

```bash
pip install -r geographic-locality/requirements.txt pytest
pytest geographic-locality/tests            # OPENDS_DIR=<openDS checkout> also validates the events
```

Tests use made-up names (`Synth-NNNN`) and mocked lookups; no test calls a real API.

## Vendored team code

`geographic_locality_mas/` holds copies of `community_context.py`, `indigenous_names.py` and the
two CSV files from the team repository (branch `feature/indigenous-place-names`);
`geographic_locality_mas/VENDORED.md` lists their sha256, and a test checks them. Change them in
the team repository, then copy them again:
`python geographic-locality/vendor_team.py ../dissco-geographic-locality-mas`.

---
*Side note: this MAS was written with help from Claude (Anthropic).*
