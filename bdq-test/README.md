# BDQ test MAS

A DiSSCo Machine Annotation Service implementing 28 selected BDQ tests, including
local validations and GBIF-backed taxonomy checks. It produces `oa:assessing`
annotations for the relevant OpenDS events, locations, identifications and taxa.
The commands below start with the simple `VALIDATION_EVENTDATE_NOTEMPTY` example.
Omit `--tests` to run all 28 tests; GBIF-backed tests need network access.

The same processing function is used for a saved JSON job, a live sandbox specimen,
and jobs received through RabbitMQ:

```text
job JSON → OpenDS fields → BDQ responses → annotation event JSON
```

For registration and testing on DiSSCo's own broker, see [Sandbox setup](SANDBOX.md).

## Run the real sandbox example

Run these commands from the repository root. The local runner uses only the Python
standard library; Python 3.12 or newer is recommended.

```sh
python3 bdq-test/main.py --specimen SANDBOX/ZJZ-NMY-GN8 --tests VALIDATION_EVENTDATE_NOTEMPTY
```

This fetches [the sandbox specimen](https://sandbox.dissco.tech/ds/SANDBOX/ZJZ-NMY-GN8)
from its [JSON API](https://sandbox.dissco.tech/api/digital-specimen/v1/SANDBOX/ZJZ-NMY-GN8),
extracts `data.attributes`, and wraps it in a local job envelope. The specimen page
URL can also be passed to `--specimen`.

The saved example was fetched on 2026-10-14 (specimen version 4). It contains one
collecting event with `dwc:eventDate: "1916-07"`, which returns:

```json
{
  "Response.status": "RUN_HAS_RESULT",
  "Response.result": "COMPLIANT",
  "Response.comment": "dwc:eventDate has a value."
}
```

Rerun the saved specimen without network access:

```sh
python3 bdq-test/main.py --input bdq-test/examples/sandbox-job.json --tests VALIDATION_EVENTDATE_NOTEMPTY
```

Refresh the saved input and output from the sandbox:

```sh
python3 bdq-test/main.py --specimen SANDBOX/ZJZ-NMY-GN8 \
  --tests VALIDATION_EVENTDATE_NOTEMPTY \
  --job-id local-SANDBOX-ZJZ-NMY-GN8 \
  --save-input bdq-test/examples/sandbox-job.json \
  --output bdq-test/examples/sandbox-annotations.json
```

`examples/sandbox-job.json` preserves the real specimen attributes. Its job ID is
generated locally for this demonstration. `examples/sandbox-annotations.json`
contains the resulting annotation event. Local runs generate JSON without
publishing annotations to DiSSCo.

## Try a missing date

```sh
python3 bdq-test/main.py --input bdq-test/examples/missing-date-job.json --tests VALIDATION_EVENTDATE_NOTEMPTY
```

This separate, synthetic specimen has an event without `dwc:eventDate`. Its result
is `RUN_HAS_RESULT` / `NOT_COMPLIANT`. The annotation selects the existing event
class because the date property does not exist. Its saved output is
`examples/missing-date-annotations.json`.

## How the annotation is built

- The incoming `jobId` is returned unchanged in `{ "jobId": ..., "annotations": [...] }`.
- Each event gets its own assessment, retaining its array index and date value.
- An existing date property uses a term selector such as
  `$['ods:hasEvents'][0]['dwc:eventDate']`.
- A missing date property uses a class selector for its event. When no events
  exist, the assessment targets the whole specimen without a selector.
- The body retains the test label, versioned test IRI, implementation version,
  input value, input path, and BDQ response.
- DiSSCo's 0.4 body schema requires strings in `oa:value`, so the structured BDQ
  result is serialized with `json.dumps`. Decode `oa:value[0]` to read it.
- `MAS_ID` and `MAS_NAME` identify the software that generated the annotation.
  Local runs use `https://example.org/mas/bdq-test` if no MAS ID is configured.

BDQ Empty includes absent/null values and strings containing only characters
U+0000 through U+0020. Strings such as `"NULL"`, `"unknown"`, and `"?"` are NotEmpty.
A nonempty value passes this particular test even if it is not a valid date.

## Check the service

The checks include BDQ empty-value cases, the real specimen snapshot, multiple
events, missing properties, and RabbitMQ acknowledgement/error handling. Some
taxonomy checks currently call live GBIF, so the full suite needs network access:

```sh
python3 -m unittest discover -s bdq-test -p 'test_*.py' -v
```

Validate the example outputs against the official DiSSCo annotation event schema
(internet access required):

```sh
python3 -m venv venv
venv/bin/python -m pip install -r bdq-test/requirements-dev.txt
venv/bin/python bdq-test/validate_output.py \
  bdq-test/examples/sandbox-annotations.json \
  bdq-test/examples/missing-date-annotations.json
```

## Run through RabbitMQ

Install `requirements.txt`, then run `python3 bdq-test/main.py --rabbitmq`, or build
the container from the repository root:

```sh
docker build -f bdq-test/Dockerfile -t bdq-test .
docker run --rm --env-file .env bdq-test
```

Configure the environment with values supplied by DiSSCo:

| Variable | Purpose / default |
| --- | --- |
| `RABBITMQ_HOST` | Required broker hostname |
| `RABBITMQ_USER` | Required broker username |
| `RABBITMQ_PASSWORD` | Required broker password |
| `RABBITMQ_QUEUE` | Required queue assigned to this MAS |
| `MAS_ID` | Required registered MAS handle, without `https://hdl.handle.net/` |
| `MAS_NAME` | Software name; default `BDQ event date test` |
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
