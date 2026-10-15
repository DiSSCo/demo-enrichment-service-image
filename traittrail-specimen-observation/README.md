# TraitTrail specimen observation MAS

Receives a digital-media job, calls a configured image-observation API, and returns openDS 0.4.0 media comments with region evidence. Predictions are explicitly unreviewed; no automatic specimen edits or taxonomic identification.

The external value service is provided by [TraitTrail Capture](https://github.com/JackDanHollister/traittrail-capture). For the hackathon it runs Qwen on the developer's GB10. No image uploads or model weights are bundled here.

## How one job works

1. DiSSCo sends a digital-media job containing the unchanged job ID, persistent media identifier and image access URL. The adapter processes one image per job; cross-resource batching is rejected.
2. The adapter downloads the image from a public HTTPS URL (maximum 10 MiB), then sends its bytes to the configured external `/infer` service with the deployment bearer token.
3. The external service checks JPEG/PNG input and image limits, retains the image/run evidence, and asks the configured VLM for the 16-feature insect observation contract. Unknown features are allowed; unsupported counts or invalid region evidence reject the result. The GB10 service accepts only one inference at a time.
4. The adapter validates the observation contract again, maps image regions to DiSSCo media comments and validates the complete annotation event against the pinned schema set. Every comment has a selector. Unresolved observations select the existing whole media object using `ods:ClassSelector` and JSONPath `$`, with no invented evidence box. Predictions for the same selector share one annotation's `oa:value` array, matching DiSSCo's identity rule (target, selector, creator and motivation). Every original feature prediction and its selected evidence remains in that array. One feature can appear in several region comments; annotation count can differ from feature count. No cropped or annotated image files are generated.
5. The adapter publishes the result under the original job ID and acknowledges the input only after broker confirmation. Processing errors produce a failure event; invalid JSON or missing job IDs are rejected. Busy/time-out/model failures currently finish as failed jobs rather than triggering an automatic inference retry.

All predictions remain unreviewed. Studio can open the exported event alongside its image and save separate human-review decisions; this adapter does not send those review decisions back to DiSSCo. The GitHub fork stores the adapter source, the planned DiSSCo deployment runs that adapter, and the external GB10 runs the VLM.

## Configuration

DiSSCo supplies MAS_ID, MAS_NAME, RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_QUEUE and RUNNING_ENDPOINT. Configure TRAITTRAIL_API_URL for the agreed HTTPS value-service address, plus TRAITTRAIL_API_TOKEN as a deployment secret matching the value service. Standard annotation/failure routing follows the existing examples. Cross-resource batching is unsupported.

The running-status request uses `{RUNNING_ENDPOINT}/{MAS_ID}/{jobId}/running`, with MAS_ID as its short Handle (for example `SANDBOX/SS6-GDM-NHR`). A full `https://hdl.handle.net/` MAS identifier is also accepted and normalised only for this request; annotation creator attribution remains unchanged. RUNNING_ENDPOINT must target the same environment as the scheduled job. HTTP/network failures from this advisory update emit `running_notification_failed` with `continuing: true`, then image analysis continues. Download, inference, validation and broker publication failures retain their normal failure handling. A callback warning still needs deployment investigation, and the intermediate RUNNING state may be absent.

Restrict this demo to the organisation/dataset agreed with the support team; tombstone the MAS and remove its secret after the hackathon. Registration, restriction and endpoint configuration remain pending.

## Validation

Use Python 3.12. Install the pinned test dependencies with `python -m pip install --only-binary=:all: --require-hashes -r traittrail-specimen-observation/requirements-dev.txt`, then run `python -m pytest -q traittrail-specimen-observation/test_mas.py`. Build from this repository root with `docker build -f traittrail-specimen-observation/Dockerfile -t traittrail-specimen-observation .`.

The runtime and test locks pin all transitive dependencies and include SHA-256 hashes from the corresponding PyPI binary-wheel releases. The container and CI require those hashes and prohibit source-package installation. Review versions and regenerate hashes when updating either lock.

Pinned schemas validate offline. The 34 tests cover explicit whole-media selectors, grouped region insights, preservation of predictions/provenance, count/visibility/evidence consistency, callback warnings, failed jobs, invalid input, publish-before-ack and secret-safe stage diagnostics. The validator refactor matches the previous implementation on 1,001 varied cases and the retained live result, with vocabulary, output schema and prompt unchanged. Live inference and confirmed MAS delivery have passed; downstream sandbox annotation acceptance remains to be checked. Input delivery is at least once; durable deduplication is future production work.

## Integration diagnostics

The container emits unbuffered startup and job-stage diagnostics to stderr. Startup reports only whether each expected setting exists, never its value. Job logs identify receipt, running notification, validation, media download, inference, annotation mapping and acknowledgement. Failures include the stage, exception type, HTTP status where available and traceback file/function/line locations. Request headers/bodies, URLs and raw exception text are excluded so that access tokens and signed media links stay out of logs.

For a failed sandbox job, provide the job Handle and deployed container tag/digest, then inspect the pod's `traittrail.mas` log entries. `running_notification_failed` is an advisory warning before image download/inference, not a model failure. A failure at `download_media` concerns the source image. An `inference` failure concerns the configured value service or its result. `TRAITTRAIL_API_URL` must be the agreed HTTPS base URL without `/infer`; the registered secret name must be `TRAITTRAIL_API_TOKEN`. If the GB10 API sees no request, inspect earlier stages and the deployed endpoint configuration first.

## Current preparation — 15 October 2026

- The adapter is separate from the Studio UI and needs no graph, model weights or user images bundled into its container.
- The protected external GB10 Qwen3.8 service was restarted and tested through ngrok. A fresh original image yielded 16 validated observations; mapping produced 20 annotations and passed the full pinned schema set. The organiser's latest annotation-processing-request schema matches pinned 0.4.0. Example target/MAS identifiers remain local fixtures, not registered PIDs.
- Set `TRAITTRAIL_API_URL` to the agreed HTTPS base URL, without `/infer`. Supply `TRAITTRAIL_API_TOKEN` separately as a secret. Do not use the ngrok URL as a media identifier or commit the token. Temporary tunnel addresses can change on restart.
- `.env.example` lists the configuration names without credentials. DiSSCo supplies the registered MAS Handle, RabbitMQ settings and running endpoint; restrict initial use to JPEG/PNG digital media of preserved insects (Insecta), with no country restriction and the organisation/dataset agreed by the organisers. Shells are outside scope.
- Human review remains a separate Studio sidecar; this adapter always marks predictions unreviewed. A structurally valid annotation is not an expert-confirmed observation.
- The workflow runs scoped lint/format checks, tests and a container build on branches and pull requests without registry secrets. After those checks pass, a push to `main` (or manual run on `main`) in `DiSSCo/demo-enrichment-service-image` publishes SHA and `latest` tags using the repository's existing AWS secret names. Forks and pull requests cannot run the publication job.
- The proposed image is `public.ecr.aws/m8w1i6p4/traittrail-specimen-observation`, following the repository's current ruler-detection pipeline. The DiSSCo team must confirm/provision this registry repository and configure registration, queue and deployment settings. No image has yet been published there.
- Registering, scheduling or submitting a live DiSSCo job has not been performed by this preparation. The developer reports supplying the API token to the support team separately; the availability window and organisation/dataset restriction remain to be agreed.
