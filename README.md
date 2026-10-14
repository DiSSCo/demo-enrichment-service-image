# demo-enrichment-service-image

## Description
This project gathers demonstrator Machine Annotation Services (MASs) and enrichment pipelines operating within the Distributed System of Scientific Collections ([DiSSCo](https://www.dissco.eu/)) and Open Digital Specimen ([openDS](https://schemas.dissco.tech/)) architecture.

Each enrichment service processes Digital Specimen or Digital Media objects to generate standardized annotations, linkage records (e.g., to external biodiversity and geoscientific databases), morphological measurements, quality assessments, and automated text transcriptions.

---

## Architecture & Program Logic

All enrichment services implement an asynchronous, event-driven processing workflow mediated by message queues:

```text
+-----------------------+
|  DiSSCo Core System   |
+-----------+-----------+
            | 1. Enqueue Job (specimen/media object + jobId)
            v
+-----------------------+
|  RabbitMQ Input Queue | (Service-specific queue)
+-----------+-----------+
            | 2. Consume message
            v
+-------------------------------------------------------------+
|                 Machine Annotation Service                  |
|                                                             |
|  a. Mark Job Running: HTTP GET to RUNNING_ENDPOINT          |
|  b. Extract target data (IDs, taxonomy, images, text)       |
|  c. Execute Domain Logic:                                   |
|     - External API Linkage (GBIF, BOLD, ENA, GeoCASe, OSM)  |
|     - Computer Vision / ML (organ detection/segmentation)   |
|     - LLM / OCR (label transcription, trait extraction)     |
|     - Data Quality Tests (TDWG BDQ rules)                   |
|     - Metadata Parsing (EXIF / Image metadata)              |
|  d. Map results to openDS Annotation Event format           |
+-----------------------------+-------------------------------+
                              |
            +-----------------+-----------------+
            | 3a. Success                       | 3b. Failure / Exception
            v                                   v
+-----------------------+           +-----------------------+
|  Annotation Exchange  |           |  Failed Job Exchange  |
| (mas-annotation-exch) |           | (mas-annotation-fail) |
+-----------+-----------+           +-----------+-----------+
            |                                   |
            v                                   v
+-------------------------------------------------------------+
|              Central Annotation Processing Service          |
+-------------------------------------------------------------+
```

---

## Message Processing Lifecycle

Each Machine Annotation Service follows a strict end-to-end processing sequence:

### 1. Ingestion & Message Parsing
- The service connects to RabbitMQ and listens to a dedicated queue (`RABBITMQ_QUEUE`).
- Incoming messages adhere to the standard MAS job envelope:
  ```json
  {
    "jobId": "8a325743-bf32-49c7-b3a1-89e738c37dfc",
    "object": { ... },
    "batchingRequested": false
  }
  ```
  where `object` contains the full openDS payload of a Digital Specimen (`https://doi.org/21.T11148/894b1e6cad57e921764e`) or Digital Media record (`https://doi.org/21.T11148/bbad8c4e101e8af01115`).

### 2. Status Tracking
- Immediately upon receiving the message, the service invokes the DiSSCo tracking endpoint via HTTP GET:
  `{RUNNING_ENDPOINT}/{MAS_ID}/{jobId}/running`
- This informs the orchestration system that processing has commenced and updates the user-facing job status.

### 3. Execution of Enrichment Logic
- The target fields are extracted from the specimen or media payload (e.g., specimen identifiers, taxonomic names, coordinates, or media `accessURI`).
- The domain-specific pipeline executes:
  - **External Knowledge Bases**: Queries APIs (such as BOLD, ENA, GeoCASe, GBIF, Mindat, Nominatim) to match identifiers or locations.
  - **Computer Vision & Morphometrics**: Downloads digital media assets and applies neural networks (e.g., YOLO organ detection, Mask R-CNN segmentation, LeafMachine trait analysis).
  - **Natural Language Processing & LLMs**: Runs OCR and multi-turn prompt pipelines (e.g., VoucherVision, SpLAT / AI-4-Specimen-Labels, OntoGPT) to transcribe labels and extract Darwin Core attributes.
  - **Biodiversity Data Quality (BDQ)**: Evaluates TDWG BDQ validation rules against specimen attributes.
  - **Metadata Extraction**: Inspects media headers and EXIF metadata.

### 4. Annotation Modeling
- Outputs are transformed into openDS-compliant `ods:Annotation` structures via the `shared` module:
  - **Agent Attribution**: Identifies the MAS software agent (`schema:SoftwareApplication`), version, and Handle persistent identifier.
  - **Motivation**: Identifies the annotation intent (e.g., `ods:adding`, `oa:assessing`, `oa:editing`, `oa:commenting`).
  - **Selector / Target**: Identifies the precise JSONPath or class selector within the digital specimen (e.g., `$['ods:hasEntityRelationships']`).
  - **Annotation Body**: Encapsulates entity relationships, newly calculated properties, or quality assessment results.
  - **Empty Results Handling**: When an external query yields no matches, an explicit "No results found" annotation is recorded to provide full provenance.

### 5. Result Publishing & Acknowledgment
- **Success**: The formatted annotation event containing `jobId` and `annotations` list is published to `RABBITMQ_EXCHANGE` (routing key `RABBITMQ_ROUTING_KEY`).
- **Error Handling**: If an unhandled exception occurs, an error payload containing `jobId` and `errorMessage` is published to `RABBITMQ_FAILED_EXCHANGE`.
- **Message Acknowledgment**: The input message is acknowledged (`basic_ack`) once publishing succeeds, ensuring reliable delivery without message loss.

---

## Configuration & Environment Variables

Services are configured at runtime via environment variables:

| Variable | Description | Example / Default |
| :--- | :--- | :--- |
| `RABBITMQ_HOST` | Hostname of the RabbitMQ message broker | `localhost` |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` |
| `RABBITMQ_USER` | RabbitMQ authentication username | `guest` |
| `RABBITMQ_PASSWORD` | RabbitMQ authentication password | `guest` |
| `RABBITMQ_QUEUE` | Specific service queue to consume from | `mas-bold-queue` |
| `RABBITMQ_EXCHANGE` | Exchange for successful annotation events | `mas-annotation-exchange` |
| `RABBITMQ_ROUTING_KEY` | Routing key for successful annotations | `mas-annotation` |
| `RABBITMQ_FAILED_EXCHANGE` | Exchange for failed job reporting | `mas-annotation-failed-exchange` |
| `RABBITMQ_FAILED_ROUTING_KEY` | Routing key for failed job reporting | `mas-annotation-failed` |
| `RUNNING_ENDPOINT` | HTTP endpoint for reporting active job status | `https://api.dissco.tech/api/v1/mas/job` |
| `MAS_ID` | Persistent Handle identifier for the MAS agent | `20.5000.1025/ABC-123-XYZ` |
| `MAS_NAME` | Human-readable name of the service | `BOLD Linkage Service` |

---

## Local Execution & Development

Services can be tested independently of RabbitMQ queues:
1. **Direct API / Sandbox Querying**: Using `--specimen` flags or `run_local()` functions with sample DiSSCo Sandbox specimens.
2. **Offline JSON Fixtures**: Providing a local test envelope via `--input <envelope.json>` to inspect resulting annotation events directly.

