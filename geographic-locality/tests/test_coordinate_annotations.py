"""Tests for geographic coordinate-validation annotations."""

import json

import coordinate_annotations


AGENT = {"@type": "prov:SoftwareAgent", "schema:name": "Geographic Validation MAS"}
TIMESTAMP = "2026-10-15T10:00:00Z"


def make_specimen(locations):
    return {
        "@id": "TEST/COORDINATES",
        "ods:type": "ods:DigitalSpecimen",
        "ods:hasEvents": [
            {
                "ods:hasLocation": {
                    "dwc:country": country,
                    "ods:hasGeoreference": {
                        "dwc:decimalLatitude": lat,
                        "dwc:decimalLongitude": lon,
                    },
                }
            }
            for country, lat, lon in locations
        ],
    }


def findings(specimen):
    annotations = coordinate_annotations.build_annotations(
        specimen, AGENT, TIMESTAMP
    )
    return [
        json.loads(a["oa:hasBody"]["oa:value"][0])
        for a in annotations
    ]


def test_consistent_coordinates():
    results = findings(make_specimen([("Germany", 50.1109, 8.6821)]))
    assert len(results) == 1
    assert results[0]["status"] == "consistent"
    assert results[0]["suggestions"] == []


def test_inconsistent_coordinates():
    results = findings(make_specimen([("Germany", 40.7128, -74.0060)]))
    assert results[0]["status"] == "inconsistent"
    assert results[0]["boundary_distance_km"] > 5


def test_missing_coordinates():
    results = findings(make_specimen([("Germany", None, None)]))
    assert results[0]["status"] == "unable_to_validate"


def test_multiple_locations_have_separate_selectors():
    specimen = make_specimen([
        ("Germany", 50.1109, 8.6821),
        ("France", 48.8566, 2.3522),
    ])

    annotations = coordinate_annotations.build_annotations(
        specimen, AGENT, TIMESTAMP
    )

    assert len(annotations) == 2
    assert [
        a["oa:hasTarget"]["oa:hasSelector"]["ods:class"]
        for a in annotations
    ] == [
        "$['ods:hasEvents'][0]['ods:hasLocation']",
        "$['ods:hasEvents'][1]['ods:hasLocation']",
    ]
