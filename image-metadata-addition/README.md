# image metadata addition enrichment service

## Description
In this example Image meta information is gathered and added to the OpenDS.

The application will get an OpenDS specimen from the queue.
It will then locate the images inside the specimen.
For each image it will test the url and if working add information about the image to the OpenDS.
It will then publish the update OpenDS to a queue so the processing service can update the object in the data storage layer.\

## Parameters
Parameters need to be set as environment variables.
This can be done by docker, docker-compose or kubernetes.
Locally this can be done by setting the variables in the run environment or use of an .env-file.

### Kafka
`KAFKA_CONSUMER_HOST` The host for the kafka consumer (for example localhost:9092)  
`KAFKA_PRODUCER_HOST` The host for the kafka producer (for example localhost:9092)
