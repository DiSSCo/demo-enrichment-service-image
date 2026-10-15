# Geographic Locality MAS: Indigenous place-name pointers (prototype)

DiSSCo MAS Hackathon 2026, Geographic place names team (Craig Frayne (lead), Lena Thöle,
Nidia Alvarez Armada, Franck Theeten, Marcus Ernst). Team repository:
https://github.com/craigmateo/dissco-geographic-locality-mas

For each location of a Digital Specimen, the MAS looks up official place names in
the Norwegian place-name register (Kartverket SSR) and posts **one comment** that
links to the register entries holding Indigenous (Sámi) names. It copies no names.

> Built without Sámi input. Every annotation says "Review status: not reviewed by
> Sámi representatives". Sámi place names are Sámi knowledge, even when they come
> from a state gazetteer, so this demo only points to the register.

## Flow

```
RabbitMQ (job + Digital Specimen)
  -> value_service.enrich()        embedded team module (indigenous_names.py),
                                   or the hosted API if VALUE_SERVICE_URL is set
  -> pointer_annotations           one oa:commenting per location: register links,
                                   ISO 639-3 codes, name status, distance, confidence
  -> assert_no_names()             refuses to publish if any Indigenous or Kven name
                                   string from the lookup is in an annotation
  -> mas-annotation-exchange       (failure exchange + job FAILED on any error)
```

| Lookup status | Comment on `$['ods:hasEvents'][i]['ods:hasLocation']` |
|---|---|
| `match` | the locality text names a place with an Indigenous-language name: links to up to 3 register entries, language codes, name status, confidence |
| `nearby` | no name match: register entries with Indigenous-language names near the coordinates, with distances (proximity only) |
| `no_match`, `no_coordinates`, `not_covered` | short message (Norway only for now; Finland needs `MML_API_KEY`) |
| no location in the record | comment on `$['ods:hasEvents']` |
| `error` (gazetteer down) | failure message, job FAILED (can be re-run) |

Kven names (a national minority language, not Indigenous) are never annotated.
Only `oa:commenting` is used: it is never merged into the record and its creator can
delete it. Note that a deleted DiSSCo annotation stays readable as a tombstone.

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
| `MML_API_KEY` | secret, optional | NLS Finland lookups. Not set for the demo: Finnish records get a "not configured" comment |
| `VALUE_SERVICE_URL` | env, optional | call the hosted value service (`POST /v1/enrich`) instead of the embedded module |
| `VALUE_SERVICE_API_KEY` | secret, optional | bearer key for that API |

## Registration (for DiSSCo)

`registration.json` (validates against openDS 0.4.0 `machine-annotation-service-request`):

- image `public.ecr.aws/dissco/geographic-locality`, batching off, not ingestion-compatible, 1 replica;
- the filter limits the MAS to three Sandbox test specimens
  (`$['dcterms:identifier']`): one match, one nearby, one no match.

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

## Vendored module

`indigenous_names.py` is an unchanged copy of
`src/geographic_locality_mas/indigenous_names.py` from the team repository, branch
`feature/indigenous-place-names`, commit `6a5cb9b`
(sha256 `52639629355fe4e349fc70f1e6862eb9d8ad7f806dd788d22a908c387d5ff78b`).
Change it in the team repository and copy it again; don't reformat it here.
