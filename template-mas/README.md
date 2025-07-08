# Machine Annotation Service Template

Thank you for your interest in developing Machine Annotation Services (MASs) for DiSSCo!

This directory contains template code to facilitate development of MASs.

For more information, see our [MAS Developers Guide](https://dissco.github.io/mas-developers-documentation/)
You can find example code in this repository.

# Getting Help

[Creating a GitHub Issue](https://github.com/DiSSCo/mas-developers-documentation/issues/new?template=Blank+issue) is the
best way to get help.

# Using This Directory

This directory is intended to be copied and used as a template for the development of MASs. When the MAS is ready for
testing, the developer should make a Pull Request on this repository with the new MAS.

The `shared` package contains code that will format resulting calculations to the openDS annotation model.

## RabbitMQ Message

Messages are sent between DiSSCo and MASs using [RabbitMQ](https://www.rabbitmq.com/), an asynchronous event messaging
platform.

The incoming message will be in the following format:

```
{
    "object": { ... },
    "jobId": "8a325743-bf32-49c7-b3a1-89e738c37dfc",
    "batchingRequested": true
}
```

Where `object` is the Digital Specimen or Digital Media in openDS, `jobID` is a UUID that must be passed back to DiSSCo,
and `batchingRequested` is an optional parameter indicating that the user has requested batching on the scheduled
annotation. A MAS must be properly configured to batch annotations. See the wiki entry for more information on batching
annotations.

## Data Model

The following table contains references to relevant schemas and a human-readable Terms reference.

| Resource                   | JSON Schema                                                                                                      | Terms Site                                                                                                       |
|----------------------------|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| Digital Specimen           | [Schema](https://schemas.dissco.tech/schemas/fdo-type/digital-specimen/0.4.0/digital-specimen.json)              | [Terms](https://terms.dissco.tech/digital-specimen-terms)                                                        |
| Digital Media              | [Schema](https://schemas.dissco.tech/schemas/fdo-type/digital-media/0.4.0/digital-media.json)                    | [Terms](https://terms.dissco.tech/digital-media-terms)                                                           |
| Annotation Event to DiSSCo | [Schema](https://schemas.dissco.tech/schemas/developer-schema/annotation/0.4.0/annotation-processing-event.json) | [Terms](https://terms.dissco.tech/annotation-terms) (Note: contains terms computed by DiSSCo as well as the MAS) |

The resulting message back to DiSSCo must comply to the `Annotation Event to DiSSCo` schema.

## GitHub Workflow file

You need to add a workflow file in the `.github/workflows` directory. Use existing workflow files as a template,
ensuring to replace all instances of the workflow's name with the name of your MAS. 