# BDQ Validator MAS

A DiSSCo Machine Annotation Service implementing **28 selected BDQ tests**, including
local data format/range validations and authoritative GBIF Backbone Taxonomy checks.
It produces `oa:assessing` annotations for relevant OpenDS events, locations,
georeferences, identifications, and taxa.

By default, omitting `--tests` runs all 28 tests (GBIF-backed checks use the live GBIF API).
You can also run a specific test or comma-separated list of tests via `--tests <LABELS>`.

The same processing function is used for a saved JSON job, a live sandbox specimen,
and jobs received through RabbitMQ:

```text
job JSON → OpenDS fields → BDQ responses → annotation event JSON
```

For detailed background and test reports, see:
- [Specimen Quality Report: SANDBOX/ZJZ-NMY-GN8](BDQ_REPORT_ZJZ-NMY-GN8.md)
- [New BDQ Tests Guide](BDQ_NEW_TESTS.md)
- [BDQ Test Candidates](BDQ_TEST_CANDIDATES.md)

## Run the real sandbox example

Run these commands from the repository root. The local runner uses only the Python
standard library; Python 3.12 or newer is recommended.

Run all 28 tests against [the sandbox specimen](https://sandbox.dissco.tech/ds/SANDBOX/ZJZ-NMY-GN8):

```sh
python3 bdq-validator/main.py --specimen SANDBOX/ZJZ-NMY-GN8
```

This fetches the specimen attributes from the [DiSSCo JSON API](https://sandbox.dissco.tech/api/digital-specimen/v1/SANDBOX/ZJZ-NMY-GN8)
and evaluates each test against the relevant fields. The specimen page URL can also be passed to `--specimen`.

You can also run a single test, such as `VALIDATION_EVENTDATE_NOTEMPTY`:

```sh
python3 bdq-validator/main.py --specimen SANDBOX/ZJZ-NMY-GN8 --tests VALIDATION_EVENTDATE_NOTEMPTY
```

### Specimen Evaluation Summary (`SANDBOX/ZJZ-NMY-GN8`)

The saved example (*Chamaecyparis pisifera* preserved specimen from Meise Botanic Garden)
evaluates against all 28 tests as follows:

| Test Group | Tests | Result on Specimen |
| --- | --- | --- |
| **📅 Collecting Event & Dates** | `VALIDATION_EVENTDATE_NOTEMPTY`<br>`VALIDATION_EVENTDATE_STANDARD`<br>`VALIDATION_EVENTDATE_INRANGE`<br>`VALIDATION_YEAR_INRANGE` | ✅ COMPLIANT (`1916-07` / `1916`) |
| **🌍 Location & Georeference** | `VALIDATION_COUNTRYCODE_STANDARD`<br>`VALIDATION_COORDINATES_NOTZERO`<br>`VALIDATION_MINELEVATION_INRANGE`<br>`VALIDATION_MAXELEVATION_INRANGE` | ✅ COMPLIANT (`BE`, `51.026111, 4.478055`)<br>⚪ INTERNAL_PREREQUISITES_NOT_MET (no elevation) |
| **🏷️ Identification & Record** | `VALIDATION_DATEIDENTIFIED_INRANGE`<br>`VALIDATION_BASISOFRECORD_STANDARD` | ⚪ INTERNAL_PREREQUISITES_NOT_MET (no dateIdentified)<br>✅ COMPLIANT (`PreservedSpecimen`) |
| **🌿 Taxon (Local Format Checks)** | `VALIDATION_TAXON_NOTEMPTY`<br>`VALIDATION_SCIENTIFICNAME_NOTEMPTY`<br>`VALIDATION_KINGDOM_NOTEMPTY`<br>`VALIDATION_TAXONRANK_NOTEMPTY`<br>`VALIDATION_SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY`<br>`VALIDATION_SCIENTIFICNAMEID_NOTEMPTY`<br>`VALIDATION_NAMEPUBLISHEDINYEAR_NOTEMPTY` | ✅ COMPLIANT (name, kingdom, rank, authorship)<br>❌ NOT_COMPLIANT (`scientificNameID` missing)<br>❌ NOT_COMPLIANT (`namePublishedInYear` missing) |
| **🧬 GBIF Backbone Validations** | `VALIDATION_KINGDOM_FOUND`<br>`VALIDATION_PHYLUM_FOUND`<br>`VALIDATION_CLASS_FOUND`<br>`VALIDATION_ORDER_FOUND`<br>`VALIDATION_FAMILY_FOUND`<br>`VALIDATION_GENUS_FOUND`<br>`VALIDATION_SCIENTIFICNAME_FOUND`<br>`VALIDATION_CLASSIFICATION_CONSISTENT`<br>`VALIDATION_TAXON_UNAMBIGUOUS` | ✅ COMPLIANT (all ranks verified in GBIF Backbone; exact match to usageKey `2683880`) |
| **💡 Taxonomic Amendments** | `AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID`<br>`AMENDMENT_SCIENTIFICNAMEID_FROM_TAXON` | ⚪ INTERNAL_PREREQUISITES_NOT_MET (name already present)<br>💡 FILLED_IN (proposes `dwc:scientificNameID: "gbif:2683880"`) |

### Example Response

For `dwc:eventDate: "1916-07"`:

```json
{
  "Response.status": "RUN_HAS_RESULT",
  "Response.result": "COMPLIANT",
  "Response.comment": "dwc:eventDate has a value."
}
```

For `AMENDMENT_SCIENTIFICNAMEID_FROM_TAXON`:

```json
{
  "Response.status": "FILLED_IN",
  "Response.result": {
    "dwc:scientificNameID": "gbif:2683880"
  },
  "Response.comment": "dwc:scientificNameID filled in from GBIF Backbone for 'Chamaecyparis pisifera (Siebold & Zucc.) Endl.'."
}
```

Rerun the saved specimen offline (for local tests):

```sh
python3 bdq-validator/main.py --input bdq-validator/examples/sandbox-job.json --tests VALIDATION_EVENTDATE_NOTEMPTY
```

Refresh the saved input and output from the sandbox:

```sh
python3 bdq-validator/main.py --specimen SANDBOX/ZJZ-NMY-GN8 \
  --tests VALIDATION_EVENTDATE_NOTEMPTY \
  --job-id local-SANDBOX-ZJZ-NMY-GN8 \
  --save-input bdq-validator/examples/sandbox-job.json \
  --output bdq-validator/examples/sandbox-annotations.json
```

`examples/sandbox-job.json` preserves the real specimen attributes. `examples/sandbox-annotations.json`
contains the resulting annotation event. Local runs generate JSON without
publishing annotations to DiSSCo.

## Try a missing date

```sh
python3 bdq-validator/main.py --input bdq-validator/examples/missing-date-job.json --tests VALIDATION_EVENTDATE_NOTEMPTY
```

This separate, synthetic specimen has an event without `dwc:eventDate`. Its result
is `RUN_HAS_RESULT` / `NOT_COMPLIANT`. The annotation selects the existing event
class because the date property does not exist. Its saved output is
`examples/missing-date-annotations.json`.

## How the annotation is built

- The incoming `jobId` is returned unchanged in `{ "jobId": ..., "annotations": [...] }`.
- Each event, location, identification, or taxon gets its own assessment with specific selectors.
- An existing property uses a term selector such as `$['ods:hasEvents'][0]['dwc:eventDate']`.
- A missing property uses a class selector for its parent object. When no class exists,
  the assessment targets the whole specimen without a selector.
- The body retains the test label, versioned test IRI, implementation version,
  input value, input path, and BDQ response.
- DiSSCo's 0.4 body schema requires strings in `oa:value`, so the structured BDQ
  result is serialized with `json.dumps`. Decode `oa:value[0]` to read it.
- `MAS_ID` and `MAS_NAME` identify the software that generated the annotation.
  Local runs use `https://example.org/mas/bdq-validator` if no MAS ID is configured.

BDQ Empty includes absent/null values and strings containing only characters
U+0000 through U+0020. Strings such as `"NULL"`, `"unknown"`, and `"?"` are NotEmpty.
A nonempty value passes this particular test even if it is not a valid date.

## Check the service

The checks include BDQ empty-value cases, the real specimen snapshot, multiple
events, missing properties, GBIF taxonomy integration, and RabbitMQ acknowledgement/error handling:

```sh
python3 -m unittest discover -s bdq-validator -p 'test_*.py' -v
```

Validate the example outputs against the official DiSSCo annotation event schema
(internet access required):

```sh
python3 -m venv venv
venv/bin/python -m pip install -r bdq-validator/requirements-dev.txt
venv/bin/python bdq-validator/validate_output.py \
  bdq-validator/examples/sandbox-annotations.json \
  bdq-validator/examples/missing-date-annotations.json
```

## Run through RabbitMQ

Install `requirements.txt`, then run `python3 bdq-validator/main.py --rabbitmq`, or build
the container from the repository root:

```sh
docker build -f bdq-validator/Dockerfile -t bdq-validator .
docker run --rm --env-file .env bdq-validator
```

Configure the environment with values supplied by DiSSCo:

| Variable | Purpose / default |
| --- | --- |
| `RABBITMQ_HOST` | Required broker hostname |
| `RABBITMQ_USER` | Required broker username |
| `RABBITMQ_PASSWORD` | Required broker password |
| `RABBITMQ_QUEUE` | Required queue assigned to this MAS |
| `MAS_ID` | Required registered MAS handle, without `https://hdl.handle.net/` |
| `MAS_NAME` | Software name; default `BDQ Validator` |
| `BDQ_TESTS` | Default `all`, or comma-separated test labels |
| `RUNNING_ENDPOINT` | Optional job-status base URL; uses the template's URL construction |
| `RABBITMQ_PORT` | Default `5672` |
| `RABBITMQ_VHOST` | Default `/` |
| `RABBITMQ_EXCHANGE` | Output exchange; default `mas-annotation-exchange` |
| `RABBITMQ_ROUTING_KEY` | Output routing key; default `mas-annotation` |
| `RABBITMQ_FAILED_EXCHANGE` | Failure exchange; default `mas-annotation-failed-exchange` |
| `RABBITMQ_FAILED_ROUTING_KEY` | Failure routing key; default `mas-annotation-failed` |

DiSSCo provisions the queues, exchanges, and bindings. The worker consumes one
job at a time, optionally marks it running, publishes its annotations, and
acknowledges the input after broker confirmation. Processing errors produce
`{ "jobId": ..., "errorMessage": ... }` on the failure exchange. Messages without
a usable job ID are rejected; configure a dead letter queue if they should be retained.
If the optional job-status request fails, the worker logs a warning and continues
the assessment so a temporary status-service outage does not fail a valid job.

Register this prototype with batching disabled and a Digital Specimen filter:

```json
{
  "$['ods:fdoType']": ["https://doi.org/21.T11148/894b1e6cad57e921764e"]
}
```

The GitHub workflow checks and builds the service. Publishing a container image
and registering the MAS require the DiSSCo team.

## Sources

- [DiSSCo MAS developer guide](https://dissco.github.io/mas-developers-documentation/)
- [BDQ quick reference](https://bdq.tdwg.org/draft/docs/terms/bdqtest/index.html)
- [BDQ implementer's guide](https://bdq.tdwg.org/draft/docs/guide/implementers/index.html)
- [DiSSCo annotation event schema 0.4](https://schemas.dissco.tech/schemas/developer-schema/annotation/0.4.0/annotation-processing-event.json)

This prototype implements selected tests from the BDQ draft. Full BDQ implementation
compliance requires the suite for at least one use case.
