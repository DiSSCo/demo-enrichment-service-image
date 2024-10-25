# herbarium sheet plant organ segmentation enrichment service

# Description
The service listens for DS of Herbarium sheet on a Kafka topic and processes the images by calling a plant organ segmentation API and republishes the enriched annotation data back to Kafka. This setup enables real-time processing of plant images from pretrained model and infer the plant organs and returns the information from the herbarium sheet. 

