"""Geographic validation of DiSSCo specimen records."""

import json
import math
from functools import lru_cache
from pathlib import Path
from pyproj import CRS, Transformer
from shapely.ops import transform
from shapely.geometry import Point, shape
from geographic_locality_mas.country_names import normalize_country

COUNTRIES_FILE = (
    Path(__file__).resolve().parents[1]
    / "data/reference/countries-10m.geojson"
)


@lru_cache(maxsize=1)
def load_countries():
    """Load country boundaries once and index them by country name."""
    with COUNTRIES_FILE.open(encoding="utf-8") as file:
        data = json.load(file)

    return {
        feature["properties"]["ADMIN"].casefold(): shape(feature["geometry"])
        for feature in data["features"]
    }


def validate_coordinates(specimen: dict) -> dict:
    """Check whether specimen coordinates fall inside its recorded country."""
    latitude = specimen.get("latitude")
    longitude = specimen.get("longitude")
    country = specimen.get("country")

    result = {
        "specimen_id": specimen.get("specimen_id"),
        "country": country,
        "latitude": latitude,
        "longitude": longitude,
        "status": "unable_to_validate",
        "reason": None,
    }

    if latitude is None or longitude is None:
        result["reason"] = "Missing latitude or longitude"
        return result

    if (
        isinstance(latitude, bool)
        or isinstance(longitude, bool)
        or not isinstance(latitude, (int, float))
        or not isinstance(longitude, (int, float))
        or not math.isfinite(latitude)
        or not math.isfinite(longitude)
    ):
        result["status"] = "invalid_coordinates"
        result["reason"] = "Coordinates must be finite numbers"
        return result

    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        result["status"] = "invalid_coordinates"
        result["reason"] = "Coordinates outside valid geographic ranges"
        return result

    if not isinstance(country, str) or not country.strip():
        result["reason"] = "Missing country"
        return result

    countries = load_countries()
    boundary = countries.get(normalize_country(country).casefold())

    if boundary is None:
        result["reason"] = "Country not found in reference dataset"
        return result

    point = Point(longitude, latitude)

    if boundary.covers(point):
        result["status"] = "consistent"
        result["reason"] = "Coordinates fall within recorded country"
    else:
        result["status"] = "inconsistent"
        result["reason"] = "Coordinates fall outside recorded country"

    return result
def suggest_longitude_sign_correction(specimen: dict) -> dict | None:
    """Suggest a longitude sign flip for a geographic mismatch.

    Returns a possible correction without modifying the specimen.
    A country match alone does not prove the correction is accurate.
    """
    original = validate_coordinates(specimen)

    if original["status"] != "inconsistent":
        return None

    longitude = specimen["longitude"]

    if longitude == 0:
        return None

    candidate = {
        **specimen,
        "longitude": -longitude,
    }

    corrected = validate_coordinates(candidate)

    if corrected["status"] != "consistent":
        return None

    return {
        "type": "possible_longitude_sign_error",
        "original_longitude": longitude,
        "suggested_longitude": -longitude,
        "corrected_status": corrected["status"],
        "requires_review": True,
    }

def suggest_coordinate_sign_corrections(specimen: dict) -> list[dict]:
    """Suggest plausible coordinate sign corrections without modifying the specimen."""
    original = validate_coordinates(specimen)

    if original["status"] != "inconsistent":
        return []

    latitude = specimen["latitude"]
    longitude = specimen["longitude"]

    candidates = [
        ("latitude_sign", -latitude, longitude),
        ("longitude_sign", latitude, -longitude),
        ("both_signs", -latitude, -longitude),
    ]

    suggestions = []
    seen = set()

    for correction_type, candidate_lat, candidate_lon in candidates:
        coordinates = (candidate_lat, candidate_lon)

        if coordinates == (latitude, longitude) or coordinates in seen:
            continue

        seen.add(coordinates)

        candidate = {
            **specimen,
            "latitude": candidate_lat,
            "longitude": candidate_lon,
        }

        result = validate_coordinates(candidate)

        if result["status"] == "consistent":
            suggestions.append({
                "type": f"possible_{correction_type}_error",
                "original_latitude": latitude,
                "original_longitude": longitude,
                "suggested_latitude": candidate_lat,
                "suggested_longitude": candidate_lon,
                "requires_review": True,
            })

    return suggestions

def distance_to_country_km(
    latitude: float,
    longitude: float,
    polygon,
) -> float:
    """Calculate approximate ground distance to a country polygon."""

    local_crs = CRS.from_proj4(
        f"+proj=aeqd +lat_0={latitude} +lon_0={longitude} "
        "+datum=WGS84 +units=m +no_defs"
    )

    transformer = Transformer.from_crs(
        "EPSG:4326",
        local_crs,
        always_xy=True,
    )

    projected_polygon = transform(transformer.transform, polygon)
    projected_point = transform(
        transformer.transform,
        Point(longitude, latitude),
    )

    return projected_point.distance(projected_polygon) / 1000


def validate_coordinates_with_tolerance(
    specimen: dict,
    tolerance_km: float = 5.0,
) -> dict:
    """Validate coordinates with a configurable country-boundary tolerance."""

    if not math.isfinite(tolerance_km) or tolerance_km < 0:
        raise ValueError("tolerance_km must be a finite non-negative number")

    result = validate_coordinates(specimen)

    result["boundary_distance_km"] = None
    result["tolerance_km"] = tolerance_km

    if result["status"] == "consistent":
        result["boundary_distance_km"] = 0.0
        return result

    if result["status"] != "inconsistent":
        return result

    latitude = specimen["latitude"]
    longitude = specimen["longitude"]
    country = specimen["country"]

    polygon = load_countries()[normalize_country(country).casefold()]

    distance = distance_to_country_km(
        latitude,
        longitude,
        polygon,
    )

    result["boundary_distance_km"] = round(distance, 3)

    if distance <= tolerance_km:
        result["status"] = "near_boundary"
        result["reason"] = (
            "Coordinates fall just outside the recorded country's "
            "reference boundary"
        )

    return result