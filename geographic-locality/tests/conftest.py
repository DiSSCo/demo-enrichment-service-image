"""Test setup: import paths, a mocked Kartverket SSR lookup and a fake RabbitMQ channel.

Run from the repository root:  pytest geographic-locality/tests
(pytest.ini puts geographic-locality/ and the repository root on the path.)
No test calls a real API. All place names are made up (Synth-NNNN); place IDs 9000xx
are not real Kartverket IDs. Area names (Karasjok) are the majority-language names in
the team's area table.
"""

import os

import pytest
from geographic_locality_mas import indigenous_names as ina

# shared.get_agent() reads these when shared is first imported (by the test module)
os.environ.setdefault("MAS_ID", "TEST/MAS-0001")
os.environ.setdefault("MAS_NAME", "Geographic Locality: Indigenous community context, Sámi (test)")

FDO_TYPE = "https://doi.org/21.T11148/894b1e6cad57e921764e"
SPECIMEN_ID = "https://doi.org/SANDBOX/TST-000-001"


def ssr_name(place_id, n, feature_type, distance_m, name, language, status="hovednavn"):
    """One name as ssr_near() returns it."""
    return {
        "place_id": place_id,
        "name_id": f"{place_id}-{n}",
        "feature_type": feature_type,
        "distance_m": distance_m,
        "name": name,
        "language": language,
        "name_status": status,
        "spelling_status": "vedtatt",
    }


# Place 900001 (town, 300 m): majority name Synth-1000, Northern Sámi Synth-0001, Kven Synth-0002.
# Place 900002 (lake, 1200 m): only a Lule Sámi name, Synth-0003.
SSR_NAMES = [
    ssr_name("900001", 1, "By", 300.0, "Synth-1000", "norsk"),
    ssr_name("900001", 2, "By", 300.0, "Synth-0001", "nordsamisk"),
    ssr_name("900001", 3, "By", 300.0, "Synth-0002", "kvensk"),
    ssr_name("900002", 1, "Innsjø", 1200.0, "Synth-0003", "lulesamisk"),
]
MADE_UP_INDIGENOUS_AND_KVEN = ["Synth-0001", "Synth-0002", "Synth-0003"]


def specimen(locality="Ved Synth-1000, nær vannet", country="Norway", coords=(69.0, 23.0), unc=707, **admin):
    """A bare openDS Digital Specimen (as RabbitMQ delivers it). Event 0 has no location.
    admin: dwc fields of the location, e.g. municipality="Karasjok"."""
    geo = (
        {}
        if coords is None
        else {
            "ods:hasGeoreference": {
                "@type": "ods:GeoReference",
                "dwc:decimalLatitude": coords[0],
                "dwc:decimalLongitude": coords[1],
                "dwc:coordinateUncertaintyInMeters": unc,
            }
        }
    )
    location = {
        "@type": "ods:Location",
        "dwc:country": country,
        "dwc:locality": locality,
        **{f"dwc:{k}": v for k, v in admin.items()},
        **geo,
    }
    return {
        "@id": SPECIMEN_ID,
        "dcterms:identifier": SPECIMEN_ID,
        "ods:fdoType": FDO_TYPE,
        "ods:organisationName": "Synthetic Herbarium",
        "ods:organisationID": "https://ror.org/000000000",
        "ods:hasEvents": [{"@type": "ods:Event"}, {"@type": "ods:Event", "ods:hasLocation": location}],
    }


class FakeChannel:
    """Stands in for a pika BlockingChannel: records what is published."""

    def __init__(self):
        self.published = []

    def basic_publish(self, exchange, routing_key, body):
        self.published.append({"exchange": exchange, "routing_key": routing_key, "body": body})


@pytest.fixture
def ssr(monkeypatch):
    """Mocked SSR (and NLS) lookup; records the calls. Set .names or .error to change the answer."""

    class Lookup:
        def __init__(self):
            self.names, self.error, self.calls = SSR_NAMES, None, []

        def __call__(self, lat, lon, radius_m=2000):
            self.calls.append((lat, lon, radius_m))
            if self.error:
                raise ina.GazetteerError(self.error)
            return list(self.names)

    fake = Lookup()
    monkeypatch.setitem(ina.LOOKUPS, "ssr", fake)
    monkeypatch.setitem(ina.LOOKUPS, "nls", fake)
    monkeypatch.delenv("VALUE_SERVICE_URL", raising=False)
    monkeypatch.delenv("MML_API_KEY", raising=False)
    return fake
