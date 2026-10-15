"""Validate saved annotation events against DiSSCo's published 0.4 schema.

Install requirements-dev.txt first. Internet access is needed to fetch schemas.
"""

import argparse
import json
from functools import lru_cache
from urllib.request import urlopen

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

SCHEMA_URL = "https://schemas.dissco.tech/schemas/developer-schema/annotation/0.4.0/annotation-processing-event.json"


@lru_cache
def retrieve_schema(uri: str) -> Resource:
    with urlopen(uri, timeout=20) as response:
        return Resource.from_contents(json.load(response))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("events", type=argparse.FileType("r", encoding="utf-8"), nargs="+")
    args = parser.parse_args()
    schema = retrieve_schema(SCHEMA_URL).contents
    validator = Draft202012Validator(schema, registry=Registry(retrieve=retrieve_schema))
    for stream in args.events:
        with stream:
            event = json.load(stream)
        validator.validate(event)
        print(f"Valid DiSSCo annotation event: {stream.name}")


if __name__ == "__main__":
    main()
