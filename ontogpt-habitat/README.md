# Herbarium Sheet Plant Organ Segmentation Enrichment Service

## Description
This service listens for Digital Specimens (DS) of habitat information on a specified RabbitMQ queue, processes the ontology extraction from habitat by calling a ontoGPT Habitat extraction API, and republishes the enriched annotation data to another RabbitMQ queue. This setup enables real-time processing of extracting ontologies from the free text and returns ontologies term.

## Environment Variables

The following environment variables must be set for the service to function correctly:

- `RABBITMQ_USER`  
- `RABBITMQ_PASSWORD`  
- `RABBITMQ_HOST`  
- `RABBITMQ_QUEUE`  
- `RABBITMQ_ROUTING_KEY`  -> defaults to `mas-annotation`
- `RABBITMQ_EXCHANGE`  -> defaults to `mas-annotation-exchange`
- `HABITAT_ONTOGPT_USER`  
- `HABITAT_ONTOGPT_PASSWORD`  
