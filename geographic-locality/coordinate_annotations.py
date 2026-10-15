"""Convert geographic coordinate validation results into DiSSCo annotations."""

import json
from typing import Any

import community_annotations
from geographic_locality_mas.extraction import extract_localities
from geographic_locality_mas.validation import (
    suggest_coordinate_sign_corrections,
    validate_coordinates_with_tolerance,
)


def build_annotations(
    specimen: dict[str, Any],
    agent: dict[str, Any],
    timestamp: str,
    tolerance_km: float = 5.0,
) -> list[dict[str, Any]]:
    """Create one location-specific commenting annotation per extracted location."""
    target = community_annotations.target_of(specimen)
    annotations = []

    for location in extract_localities(specimen):
        result = validate_coordinates_with_tolerance(location, tolerance_km)

        suggestions = (
            suggest_coordinate_sign_corrections(location)
            if result["status"] == "inconsistent"
            else []
        )

        finding = {
            "method": "Natural Earth country-boundary validation",
            "status": result["status"],
            "reason": result["reason"],
            "country": result["country"],
            "latitude": result["latitude"],
            "longitude": result["longitude"],
            "boundary_distance_km": result["boundary_distance_km"],
            "tolerance_km": result["tolerance_km"],
            "suggestions": suggestions,
            "coordinate_source": location["coordinate_source"],
            "distance": location["distance"],
        }

        annotations.append(
            community_annotations.comment(
                agent,
                timestamp,
                [json.dumps(finding, ensure_ascii=False)],
                location["target"],
                target,
                "",
            )
        )

    return annotations
