# Herbarium Sheet Plant Organ Segmentation Enrichment Service

## Description
This service listens for Digital Specimens (DS) of herbarium sheets on a specified RabbitMQ queue, processes the images by calling a plant organ segmentation API, and republishes the enriched annotation data to another RabbitMQ queue. This setup enables real-time processing of herbarium sheet images using a pretrained model to detect and analyze plant organs, returning detailed information about the plant specimen.

## Environment Variables

The following environment variables must be set for the service to function correctly:

- `RABBITMQ_USER`  
- `RABBITMQ_PASSWORD`  
- `RABBITMQ_HOST`  
- `RABBITMQ_QUEUE`  
- `RABBITMQ_ROUTING_KEY`  -> defaults to `mas-annotation`
- `RABBITMQ_EXCHANGE`  -> defaults to `habitat-enrichment-exchange`
- `PLANT_ORGAN_SEGMENTATION_USER`  
- `PLANT_ORGAN_SEGMENTATION_PASSWORD`  
