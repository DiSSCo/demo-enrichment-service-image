# Ruler Detection Enrichment Service

## Description
This service listens for Digital Media of herbarium sheets on a RabbitMQ queue and sends the image to the
[scale detection service](https://github.com/RajapreethiRajendran/DiSSCo-MAS-Pilot). That service finds the ruler on
the sheet and reads how many pixels make one centimetre.

Each ruler with a verified reading becomes two annotations, both with motivation `ods:adding` and the detection confidence in
`ods:score`:
- **Region**: an `oa:FragmentSelector` on the ruler, so its bounding box is shown on the image. The value holds the
  bounding box and the pixel per cm reading.
- **Assertion**: an `ods:ClassSelector` on `$['ods:hasAssertions'][n]`, so the reading is added to the media object.
  The value is an `ods:Assertion` with `dwc:measurementType` `pixels per centimetre` and `dwc:measurementUnit` `px/cm`.

Unverified readings are skipped. If there is no verified reading, the service sends a single `oa:commenting` annotation instead.

## Environment Variables

- `RABBITMQ_USER`
- `RABBITMQ_PASSWORD`
- `RABBITMQ_HOST`
- `RABBITMQ_QUEUE`
- `RABBITMQ_ROUTING_KEY` -> defaults to `mas-annotation`
- `RABBITMQ_EXCHANGE` -> defaults to `mas-annotation-exchange`
- `SCALE_DETECTION_URL` -> defaults to `https://webapp.senckenberg.de/dissco-ruler-detection-mas-proto/scale_detection`
- `SCALE_DETECTION_USER`
- `SCALE_DETECTION_PASSWORD`
