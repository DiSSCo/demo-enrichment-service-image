# Register and test the BDQ MAS in DiSSCo's sandbox

DiSSCo's team registers and deploys MASs through its orchestration service. The
[registration guide](https://dissco.github.io/mas-developers-documentation/guide/registration)
asks providers to submit the service in a pull request, with a Dockerfile,
requirements and workflow, and supply service metadata and filters.

The repository's `mas-ingestion/mas_ingestion.py` contains authenticated
registration examples. Its current entry point updates an unrelated existing MAS;
use it as a reference for the request format.

## Handoff to the DiSSCo team

Push the reviewed fixes to the branch and open the pull request requested by the
registration guide before asking the team to build and deploy from it.

You can send the following request to the team:

> Please register and deploy our BDQ MAS in the sandbox from the `bdq-test` branch
> of `DiSSCo/demo-enrichment-service-image`, service directory `bdq-test`.
> The Dockerfile is `bdq-test/Dockerfile`, with the repository root as build context.
> It assesses Digital Specimens using 28 selected BDQ tests, including GBIF-backed
> checks, and returns `oa:assessing` annotations. Please enable the Digital Specimen
> filter below and disable batching. Could you provide the registered MAS ID and
> confirm how we should schedule and observe a test job for `SANDBOX/ZJZ-NMY-GN8`?

Suggested service name: **BDQ data quality tests**.

```json
{
  "$['ods:fdoType']": ["https://doi.org/21.T11148/894b1e6cad57e921764e"]
}
```

Use these service settings:

- Container command: `python main.py --rabbitmq` (already the image default).
- Batching: disabled.
- `BDQ_TESTS=all` enables all 28 tests. For the initial connection check, use
  `BDQ_TESTS=VALIDATION_EVENTDATE_NOTEMPTY` to run one test without GBIF calls.
- Set `MAS_NAME=BDQ data quality tests` and the real registered `MAS_ID`.
- The service needs outbound HTTPS access to GBIF when GBIF-backed tests are enabled.

Build the image locally with:

```sh
docker build -f bdq-test/Dockerfile -t bdq-test .
```

The current BDQ workflow checks and builds the image. The team must arrange its
publication to a registry accessible to their deployment before registering the
image reference; the local `bdq-test` tag is not a published registry image.

## Broker configuration

The public repository supplies the messaging contract, not shared credentials.
DiSSCo injects broker settings when it deploys a registered MAS. If they want us to
run the worker locally against their broker, ask them for:

- Broker hostname, port, virtual host and network access method (for example, a
  port forward if the broker is only reachable inside their cluster).
- Credentials scoped to this MAS and its assigned input queue.
- `MAS_ID`, `MAS_NAME`, and the confirmed `RUNNING_ENDPOINT` base URL.
- Output and failure exchange/routing-key settings if their environment overrides
  the defaults in the table in [README.md](README.md).

Place the supplied values in a local `.env` file and run:

```sh
docker run --rm --env-file .env bdq-test
```

The Docker `--env-file` option loads that file. Running the Python script directly
requires the variables to be exported into its environment.

## Real sandbox test procedure

1. Have the team deploy the fixed image with the registered identity and assigned
   queue. For the first test, select `VALIDATION_EVENTDATE_NOTEMPTY`.
2. Sign into [DiSSCover sandbox](https://sandbox.dissco.tech) with an account linked
   to ORCID and open [the example specimen](https://sandbox.dissco.tech/ds/SANDBOX/ZJZ-NMY-GN8).
3. Open **Annotate → Machine Annotation Services → Schedule a MAS**, select the
   registered BDQ service, and schedule it. The team can also schedule via the
   authenticated API.
4. Record the actual scheduled `jobId`. The incoming message will contain that ID
   and the specimen in `object`. Let the worker consume this actual scheduled job.
5. Confirm the worker publishes an annotation event to the configured annotation
   exchange and acknowledges its input after broker confirmation.
6. Confirm DiSSCo marks the job complete and shows the resulting assessment on the
   specimen. With the saved version-4 specimen's date `1916-07`, the presence test
   returns `COMPLIANT`; a live specimen may have changed since that snapshot.
7. Repeat with `BDQ_TESTS=all` and inspect the full assessment report.

Scheduling steps are described in the official
[MAS scheduling guide](https://dissco.github.io/mas-developers-documentation/guide/scheduling).
Local CLI examples use synthetic job IDs; actual broker tests should use a job
scheduled by DiSSCo so completion can be correlated to its job record.
