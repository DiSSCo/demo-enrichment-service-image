# Herbarium Sheet Leafmachine Enrichment Service
- Author: Kenzo Milleville
- LeafMachine2 author: William Weaver (https://leafmachine.org/) 


## Description
This service is intended as a demo to test the integration of an internal API at Ghent University with the Disscover platform as a MAS.

This service listens for Digital Specimens (DS) of herbarium sheets on a specified RabbitMQ queue, processes the images by running the [LeafMachine2](https://github.com/Gene-Weaver/LeafMachine2) model  from an API, and republishes the enriched annotation data to another RabbitMQ queue. This setup enables real-time processing of herbarium sheet images using a pretrained model to detect and analyze plant organs, returning detailed information about the plant specimen.

## Environment Variables

The following environment variables must be set for the service to function correctly:

- `RABBITMQ_USER`  
- `RABBITMQ_PASSWORD`  
- `RABBITMQ_HOST`  
- `RABBITMQ_QUEUE`  
- `RABBITMQ_ROUTING_KEY`  -> defaults to `mas-annotation`
- `RABBITMQ_EXCHANGE`  -> defaults to `habitat-enrichment-exchange` 
