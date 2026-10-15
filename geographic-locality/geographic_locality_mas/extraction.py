"""Extract structured geographic context from DiSSCo specimen records."""


def extract_localities(specimen):
    attributes = specimen.get("attributes") or specimen
    specimen_id = specimen.get("id") or specimen.get("@id")
    results = []

    for event_index, event in enumerate(attributes.get("ods:hasEvents", [])):
        location = event.get("ods:hasLocation") or {}
        georef = location.get("ods:hasGeoreference") or {}

        if not location:
            continue

        results.append({
            "specimen_id": specimen_id,
            "country": location.get("dwc:country"),
            "locality": location.get("dwc:locality"),
            "latitude": georef.get("dwc:decimalLatitude"),
            "longitude": georef.get("dwc:decimalLongitude"),
            "event_index": event_index,
            "target": f"$['ods:hasEvents'][{event_index}]['ods:hasLocation']",
        })

    return results

def test_extracts_bare_opends_specimen():
    specimen = {
        "@id": "https://doi.org/SANDBOX/TEST-123",
        "ods:hasEvents": [
            {
                "ods:hasLocation": {
                    "dwc:country": "Greece",
                    "dwc:locality": "Milos",
                    "ods:hasGeoreference": {
                        "dwc:decimalLatitude": 36.74,
                        "dwc:decimalLongitude": 24.43,
                    },
                }
            }
        ],
    }

    result = extract_localities(specimen)

    assert len(result) == 1
    assert result[0]["specimen_id"] == "https://doi.org/SANDBOX/TEST-123"
    assert result[0]["country"] == "Greece"
    assert result[0]["locality"] == "Milos"
    assert result[0]["latitude"] == 36.74
    assert result[0]["longitude"] == 24.43