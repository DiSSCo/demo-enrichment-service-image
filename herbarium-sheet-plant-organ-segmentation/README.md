# Herbarium Sheet Plant Organ Segmentation Enrichment Service

## Description
This service listens for Digital Specimens (DS) of herbarium sheets on a specified Kafka topic, processes the images by calling a plant organ segmentation API, and republishes the enriched annotation data to another Kafka topic. This setup enables real-time processing of herbarium sheet images using a pretrained model to detect and analyze plant organs, returning detailed information about the plant specimen.

## Environment Variables

The following environment variables must be set for the service to function correctly:

- `KAFKA_CONSUMER_TOPIC`  
- `KAFKA_CONSUMER_GROUP`  
- `KAFKA_CONSUMER_HOST`  
- `KAFKA_PRODUCER_HOST`  
- `KAFKA_PRODUCER_TOPIC`  
- `PLANT_ORGAN_SEGMENTATION_USER`  
- `PLANT_ORGAN_SEGMENTATION_PASSWORD`  
