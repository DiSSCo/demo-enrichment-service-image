# Herbarium Sheet Plant Organ Segmentation Enrichment Service

## Description
This service listens for Digital Specimens (DS) of habitat information on a specified Kafka topic, processes the ontology extraction from habitat by calling a ontoGPT Habitat extraction API, and republishes the enriched annotation data to another Kafka topic. This setup enables real-time processing of extracting ontologies from the free text and returns ontologies term.

## Environment Variables

The following environment variables must be set for the service to function correctly:

- `KAFKA_CONSUMER_TOPIC`  
- `KAFKA_CONSUMER_GROUP`  
- `KAFKA_CONSUMER_HOST`  
- `KAFKA_PRODUCER_HOST`  
- `KAFKA_PRODUCER_TOPIC`  
- `PLANT_ORGAN_SEGMENTATION_USER`  
- `PLANT_ORGAN_SEGMENTATION_PASSWORD`  
