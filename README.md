# demo-enrichment-service-image

## Description
This project gathers demonstrators for image enrichment services within the Open Digital Specimen architecture. All enrichment services use a separate RabbitMQ queue for the data they have to process, and afterwards push their enriched DS data back to the central Kafka queue of the processing service.
