"""GBIF Backbone Taxonomy API client for BDQ tests."""

import json
import logging
import re
import urllib.parse
from urllib.error import HTTPError
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)

GBIF_BACKBONE_DATASET_KEY = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"
GBIF_API_BASE = "https://api.gbif.org/v1"
DEFAULT_TIMEOUT = 10.0

_CACHE: dict[str, dict | list | None] = {}


class GBIFServiceError(Exception):
    """Raised when the GBIF Backbone service is unavailable or returns an error."""


def clear_cache() -> None:
    """Clear in-memory request cache."""
    _CACHE.clear()


def parse_gbif_key(identifier: str | int | None) -> int | None:
    """Extract a numeric GBIF taxon key from an ID, URN, or URL."""
    if identifier is None:
        return None
    if isinstance(identifier, int):
        return identifier if identifier > 0 else None
    if not isinstance(identifier, str):
        return None
    val = identifier.strip()
    if not val:
        return None
    # Matches URLs like https://www.gbif.org/species/2683880 or http://api.gbif.org/v1/species/2683880
    # or prefixes like gbif:2683880 or raw digits 2683880
    match = re.search(r"(?:gbif\.org/species/|api\.gbif\.org/v1/species/|^gbif:|^)(\d+)$", val, re.IGNORECASE)
    if match:
        try:
            key = int(match.group(1))
            return key if key > 0 else None
        except ValueError:
            return None
    return None


def _fetch_json(url: str, timeout: float = DEFAULT_TIMEOUT) -> dict | list | None:
    if url in _CACHE:
        return _CACHE[url]
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "bdq-validator/0.1.0 (DiSSCo BDQ MAS; +https://dissco.eu)",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            data = json.load(response)
            _CACHE[url] = data
            return data
    except HTTPError as error:
        if error.code in (400, 404):
            _CACHE[url] = None
            return None
        raise GBIFServiceError(f"GBIF API returned HTTP {error.code}: {error.reason}") from error
    except OSError as error:
        raise GBIFServiceError(f"GBIF API connection error: {error}") from error
    except ValueError as error:
        raise GBIFServiceError(f"GBIF API invalid JSON response: {error}") from error


def query_species(
    name: str,
    dataset_key: str = GBIF_BACKBONE_DATASET_KEY,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[dict]:
    """Query /species in the specified dataset (default: GBIF Backbone)."""
    if not name or not str(name).strip():
        return []
    url = f"{GBIF_API_BASE}/species?datasetKey={dataset_key}&name={urllib.parse.quote(str(name).strip())}"
    data = _fetch_json(url, timeout=timeout)
    if isinstance(data, dict):
        return data.get("results", [])
    return []


def get_species_by_key(key: int | str, timeout: float = DEFAULT_TIMEOUT) -> dict | None:
    """Fetch species record by GBIF taxon key."""
    if not key:
        return None
    url = f"{GBIF_API_BASE}/species/{urllib.parse.quote(str(key).strip())}"
    data = _fetch_json(url, timeout=timeout)
    if isinstance(data, dict):
        return data
    return None


def match_species(
    name: str | None = None,
    kingdom: str | None = None,
    phylum: str | None = None,
    class_: str | None = None,
    order: str | None = None,
    family: str | None = None,
    genus: str | None = None,
    rank: str | None = None,
    strict: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict:
    """Match a taxon name and higher classification against GBIF Backbone (/species/match)."""
    supplied = {
        "name": name,
        "kingdom": kingdom,
        "phylum": phylum,
        "class": class_,
        "order": order,
        "family": family,
        "genus": genus,
        "rank": rank,
    }
    params = {key: str(value).strip() for key, value in supplied.items() if value and str(value).strip()}
    if strict:
        params["strict"] = "true"

    if not params:
        return {"matchType": "NONE", "confidence": 0}

    qs = urllib.parse.urlencode(params)
    url = f"{GBIF_API_BASE}/species/match?{qs}"
    data = _fetch_json(url, timeout=timeout)
    if isinstance(data, dict):
        return data
    return {"matchType": "NONE", "confidence": 0}
