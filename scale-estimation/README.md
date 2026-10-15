# Image Scale Estimation Service

- Author: Kenzo Milleville

## Description

This service processes images by calling the scale estimation API at IDLab, and republishes the enriched annotation data to Dissco.

The request payload is only the image URL:

```json
{ "image_url": "https://example.org/specimen.jpg" }
```

Endpoint: `https://herbaria.idlab.ugent.be/api/mas-scale-estimation`

Each detection becomes an annotation with `boundingBox`, `class`, and `score`, using the original image shape from the response metadata.

## Local run

Uncomment `run_local` in `main.py`. The annotation event is pretty-printed and written to `temp_output.json` in the working directory.

## Source code

The scale estimation code can be found at: https://github.com/kymillev/scale-estimation
