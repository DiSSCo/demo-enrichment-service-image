"""Extract structured geographic context from DiSSCo specimen records."""

from .coord_extraction import extract_coordinates
from .dist_extraction import extract_distance


def extract_localities(specimen):
    attributes = specimen.get("attributes") or specimen
    specimen_id = specimen.get("id") or specimen.get("@id")
    results = []

    for event_index, event in enumerate(attributes.get("ods:hasEvents", [])):
        location = event.get("ods:hasLocation") or {}
        if not location:
            continue

        georef = location.get("ods:hasGeoreference") or {}
        locality = location.get("dwc:locality")
        latitude = georef.get("dwc:decimalLatitude")
        longitude = georef.get("dwc:decimalLongitude")
        original_latitude = latitude
        original_longitude = longitude
        original_latitude = latitude
        original_longitude = longitude
        coordinate_source = "specimen"

        # Derive missing coordinates without modifying the source specimen.
        if latitude is None or longitude is None:
            coordinates = extract_coordinates({
                "locality": locality,
                "latitude": latitude,
                "longitude": longitude,
            })
            latitude = coordinates.get("latitude")
            longitude = coordinates.get("longitude")
            if latitude is not None and longitude is not None:
                if original_latitude is None and original_longitude is None:
                    coordinate_source = "locality_text"
                elif original_latitude is None or original_longitude is None:
                    coordinate_source = "mixed"

        # Extract distance context without modifying the source specimen.
        distance = location.get("ods:hasDistance")
        if distance is None:
            extracted = extract_distance({"locality": locality})
            distance = extracted.get("distance")

        results.append({
            "specimen_id": specimen_id,
            "country": location.get("dwc:country"),
            "locality": locality,
            "latitude": latitude,
            "longitude": longitude,
            "distance": distance,
            "coordinate_source": coordinate_source,
            "event_index": event_index,
            "target": f"$['ods:hasEvents'][{event_index}]['ods:hasLocation']",
        })

    return results
