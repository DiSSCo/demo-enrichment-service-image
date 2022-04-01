# herbarium sheet plant organ detection enrichment service

## Description
In this example the herbarium sheet image of a DS is fed into a previously trained neural network for plant organ detection (e.g. leaf,stem,flower, etc.). The trained neural network can be found [here](https://github.com/2younis/plant-organ-detection) and is described in [this paper from Younis et al. 2020](https://arxiv.org/abs/2007.13106).

## Parameters
Parameters need to be set as environment variables.
This can be done by docker, docker-compose or kubernetes.
Locally this can be done by setting the variables in the run environment or use of an .env-file.

### Kafka
`KAFKA_CONSUMER_HOST` The host for the kafka consumer (for example localhost:9092)  
`KAFKA_PRODUCER_HOST` The host for the kafka producer (for example localhost:9092)
`KAFKA_CONSUMER_TOPIC` The topic name of the input kafka queue
`KAFKA_PRODUCER_TOPIC` The topic name of the output kafka queue
