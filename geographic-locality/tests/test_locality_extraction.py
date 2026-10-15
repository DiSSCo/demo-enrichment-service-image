"""Regression tests for coordinate and distance extraction."""

from copy import deepcopy

from geographic_locality_mas.extraction import extract_localities


def make_specimen(locality, latitude=None, longitude=None):
    return {
        "id": "test-specimen",
        "attributes": {
            "ods:hasEvents": [{
                "ods:hasLocation": {
                    "dwc:country": "Germany",
                    "dwc:locality": locality,
                    "ods:hasGeoreference": {
                        "dwc:decimalLatitude": latitude,
                        "dwc:decimalLongitude": longitude,
                    },
                }
            }]
        },
    }


def test_extract_coordinates_without_mutating_specimen():
    specimen = make_specimen("Frankfurt: 50.1109, 8.6821")
    original = deepcopy(specimen)

    location = extract_localities(specimen)[0]

    assert location["latitude"] == 50.1109
    assert location["longitude"] == 8.6821
    assert location["coordinate_source"] == "locality_text"
    assert specimen == original


def test_preserve_existing_coordinates():
    specimen = make_specimen("Frankfurt am Main", 50.1109, 8.6821)

    location = extract_localities(specimen)[0]

    assert location["latitude"] == 50.1109
    assert location["longitude"] == 8.6821
    assert location["coordinate_source"] == "specimen"


def test_mixed_coordinate_sources():
    specimen = make_specimen("Frankfurt: 50.1109, 8.6821", latitude=50.1109)

    location = extract_localities(specimen)[0]

    assert location["longitude"] == 8.6821
    assert location["coordinate_source"] == "mixed"


def test_extract_distance():
    specimen = make_specimen("5 km southwest of Frankfurt")

    location = extract_localities(specimen)[0]

    assert location["distance"] == {"value": 5.0, "unit": "km"}


def test_locality_without_coordinates_or_distance():
    specimen = make_specimen("Frankfurt am Main")

    location = extract_localities(specimen)[0]

    assert location["latitude"] is None
    assert location["longitude"] is None
    assert location["distance"] is None
    assert location["coordinate_source"] == "specimen"
