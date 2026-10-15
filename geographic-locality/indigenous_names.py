"""Indigenous place names for one specimen location.

DiSSCo MAS Hackathon 2026 - Geographic Place Names team.
Workstream: Indigenous place names.

Python twin of R/indigenous_names.R, which is kept outside this repo. Same
function names, same outputs: both are tested on the same mocked gazetteer
responses (tests/fixtures/).

find_indigenous_names(location) takes one specimen location and returns the
Indigenous-language names of the places its locality text names, from the
official state gazetteer of that country:

    "Kilpisjärvi" (locality) -> NLS place 10785565 -> its Northern Sámi name (sme)

Gazetteers (both CC BY 4.0):
- Norway:  Kartverket, Sentralt stedsnavnregister (SSR). No key.
- Finland: National Land Survey of Finland (NLS), Geographic Names Register.
           Needs a free API key in the environment variable MML_API_KEY.

Rules:
- State gazetteer names are official names, not community-sourced, but Sámi
  place names are still Sámi knowledge. Every match carries its source,
  language, name status, place ID and caveats, and the API response and every
  annotation say REVIEW_STATUS. No Sámi or Kven names are stored in the repo:
  the benchmark keeps gazetteer IDs, tests and examples use made-up names.
- Kven (Kvensk) is a national minority language, not an Indigenous one. Kven
  names come back separately (minority_language_names), never as matches.
- SSR address names ("Adressenavn") are skipped. SSR names with status
  feilført (wrongly registered) or avslått (rejected) are dropped; historical
  names are kept with a caveat and half the score.
- API keys are read from environment variables only. They never appear in
  results, error messages or logs.

Sections:
  1. Gazetteers and languages
  2. Helpers and HTTP
  3. Gazetteer lookups (ssr_near, mml_near)
  4. Matching (names_in_text, summarise_hits)
  5. Results (score_match, caveats_for, to_matches, find_indigenous_names)
"""

import math
import os
import re
import threading
import time
import unicodedata
from datetime import UTC, datetime
from urllib.parse import unquote, urlparse

import requests

# %% 1. Gazetteers and languages -------------------------------------------------

SSR_API = "https://ws.geonorge.no/stedsnavn/v1"
MML_API = ("https://avoin-paikkatieto.maanmittauslaitos.fi/"
           "geographic-names/features/v1/collections/placenames/items")

# place_uri: checked 14 Oct 2026.
# - SSR: Kartverket's fact page per stedsnummer (263614 -> Vardø).
# - NLS: persistent URI, redirects to the NLS place information card
#   (tietokortit.maanmittauslaitos.fi/nimisto/paikka/<placeId>).
GAZETTEERS = {
    "ssr": {
        "label": "Kartverket SSR",
        "title": "Kartverket, Sentralt stedsnavnregister (SSR)",
        "max_radius_m": 5000,  # /punkt returns places within <= 5000 m
        "place_uri": "https://stadnamn.kartverket.no/fakta/{place_id}",
        "licence": "https://creativecommons.org/licenses/by/4.0/",
        "attribution": "© Kartverket, Sentralt stedsnavnregister (SSR), CC BY 4.0",
    },
    "nls": {
        "label": "NLS Finland Geographic Names",
        "title": "National Land Survey of Finland, Geographic Names Register",
        "max_radius_m": 10000,
        "place_uri": "http://paikkatiedot.fi/so/1000772/{place_id}",
        "licence": "https://creativecommons.org/licenses/by/4.0/",
        "attribution": ("Contains data from the National Land Survey of Finland "
                        "Geographic Names Register {month}/{year}, CC BY 4.0"),
    },
}

# Country field -> gazetteer (case-insensitive regex on country_code, then country)
COUNTRY_GAZETTEER = [
    ("ssr", r"^(no|nor)$|norway|norge|noreg|norga|norwegen"),
    ("nls", r"^(fi|fin)$|finland|suomi|suopma|finnland"),
]

INDIGENOUS = "indigenous"
MINORITY = "national_minority"
OTHER = "other"

# Prototype rule: every API response and every annotation says this. The design
# and the names have not been reviewed by Sámi representatives yet.
REVIEW_STATUS = "not reviewed by Sámi representatives"

# Gazetteer language value (any case) -> ISO 639-3 code, English name, category.
# SSR uses Norwegian labels, NLS uses ISO 639-3 codes.
LANGUAGES = {
    "nordsamisk": ("sme", "Northern Sámi", INDIGENOUS),
    "lulesamisk": ("smj", "Lule Sámi", INDIGENOUS),
    "sørsamisk": ("sma", "Southern Sámi", INDIGENOUS),
    "skoltesamisk": ("sms", "Skolt Sámi", INDIGENOUS),
    "pitesamisk": ("sje", "Pite Sámi", INDIGENOUS),
    "umesamisk": ("sju", "Ume Sámi", INDIGENOUS),
    "kvensk": ("fkv", "Kven", MINORITY),  # national minority language, not Indigenous
    "norsk": ("nor", "Norwegian", OTHER),
    "finsk": ("fin", "Finnish", OTHER),
    "svensk": ("swe", "Swedish", OTHER),
    "russisk": ("rus", "Russian", OTHER),
    "sme": ("sme", "Northern Sámi", INDIGENOUS),
    "smn": ("smn", "Inari Sámi", INDIGENOUS),
    "sms": ("sms", "Skolt Sámi", INDIGENOUS),
    "fin": ("fin", "Finnish", OTHER),
    "swe": ("swe", "Swedish", OTHER),
}

EXCLUDE_TYPES = ("Adressenavn",)          # SSR street and address names
REJECTED_STATUS = ("feilført", "avslått")  # SSR: wrongly registered, rejected
HISTORICAL_STATUS = ("historisk",)         # SSR: not authorised for public use today

NAME_MATCH_SCORE = 0.9   # locality text names a place that has an Indigenous name
PROXIMITY_SCORE = 0.5    # at most, for an Indigenous name that is only nearby
MAX_NEARBY = 3

SUMMARY_FIELDS = ["matched_name", "indigenous_parallel", "parallel_language",
                  "gazetteer_place_id", "nearest_indigenous_name",
                  "nearest_distance_m", "n_names", "n_indigenous"]


# %% 2. Helpers and HTTP ----------------------------------------------------------

def first(x):
    """First element as text, None if missing. R: chr1()."""
    if isinstance(x, list):
        return first(x[0]) if x else None
    return None if x is None else str(x)


def num1(x):
    """x as a finite number, None otherwise."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def nfc(x):
    """Unicode NFC, so that composed and decomposed letters (á vs a + ´) match."""
    return unicodedata.normalize("NFC", x) if isinstance(x, str) else x


def join_unique(values):
    """Unique non-empty values in order, joined with ' | '. None if there are none."""
    return " | ".join(dict.fromkeys(v for v in values if v)) or None


def round_m(x):
    """Distance in whole metres (None stays None)."""
    v = num1(x)
    return None if v is None else round(v)


def round2(x):
    """Round half up to 2 decimals, the same way in R and Python (R: round2())."""
    return math.floor(x * 100 + 0.5) / 100


def fmt_m(x):
    """'2000 m'."""
    return f"{num1(x):.0f} m"


def language_info(value):
    """(ISO 639-3 code, English name, category) for a gazetteer language value."""
    if not value:
        return (None, None, OTHER)
    return LANGUAGES.get(str(value).casefold(), (None, str(value), OTHER))


def status_flag(status):
    """'rejected', 'historical' or None for a gazetteer name status."""
    s = (status or "").casefold()
    if s.startswith(REJECTED_STATUS):
        return "rejected"
    if s.startswith(HISTORICAL_STATUS):
        return "historical"
    return None


def place_uri(place_id, gaz):
    """Resolvable URI of a gazetteer place. None if the ID is not a plain number."""
    pid = (place_id or "").removeprefix("P_")
    if not re.fullmatch(r"[0-9]+", pid):
        return None
    return GAZETTEERS[gaz]["place_uri"].format(place_id=pid)


def attribution(gaz, today):
    return GAZETTEERS[gaz]["attribution"].format(month=f"{today.month:02d}", year=today.year)


def gazetteer_for(location):
    """'ssr', 'nls' or None, from country_code or country."""
    for field in ("country_code", "country"):
        value = str(location.get(field) or "").strip()
        for gaz, pattern in COUNTRY_GAZETTEER:
            if value and re.search(pattern, value, flags=re.IGNORECASE):
                return gaz
    return None


def haversine_m(lat1, lon1, lat2, lon2):
    rad = math.pi / 180
    a = (math.sin((lat2 - lat1) * rad / 2) ** 2
         + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin((lon2 - lon1) * rad / 2) ** 2)
    return 2 * 6371008.8 * math.asin(min(1.0, math.sqrt(a)))


def radius_for(unc, min_m=2000, max_m=5000):
    """Search radius: the coordinate uncertainty, kept between min_m and max_m."""
    unc = num1(unc)
    if unc is None:
        unc = min_m
    return min(max_m, max(min_m, unc))


def bbox_for(lat, lon, radius_m):
    """lon/lat box (CRS84) around a circle, as the NLS bbox string."""
    dlat = radius_m / 111320
    dlon = radius_m / (111320 * math.cos(lat * math.pi / 180))
    return ",".join(f"{v:.6f}" for v in (lon - dlon, lat - dlat, lon + dlon, lat + dlat))


def query_url(gaz, lat, lon, radius_m):
    """The first lookup request as a URL, without the API key (provenance)."""
    if gaz == "ssr":
        return (f"{SSR_API}/punkt?nord={lat:.15g}&ost={lon:.15g}&koordsys=4258"
                f"&radius={radius_m:.0f}&utkoordsys=4258&treffPerSide=500&side=1")
    return f"{MML_API}?bbox={bbox_for(lat, lon, radius_m)}&limit=1000"


class GazetteerError(RuntimeError):
    """A gazetteer lookup failed: HTTP error, missing key or a bad response."""


def redact(text):
    """Text with API keys removed. Use on anything that may reach a log or a user."""
    text = str(text)
    key = os.environ.get("MML_API_KEY")
    if key:
        text = text.replace(key, "***")
    return re.sub(r"(api-key=)[^&\s'\"]+", r"\1***", text)


SESSION = requests.Session()
SESSION.headers["User-Agent"] = "DiSSCo MAS hackathon - Geographic Place Names team"
_LAST_CALL = {}
_LOCK = threading.Lock()


def throttle(host, rate):
    """Wait so that calls to one host stay at or below `rate` per second."""
    with _LOCK:
        wait = 1 / rate - (time.monotonic() - _LAST_CALL.get(host, -math.inf))
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL[host] = time.monotonic()


def api_get(url, path=None, rate=5, **params):
    """GET a JSON API and return the parsed body (dicts and lists).

    R: api_get() with httr2. Throttles per host (R: req_throttle), retries
    429/503 (R: req_retry). Raises GazetteerError; messages never hold a key.
    """
    if path:
        url = f"{url}/{path}"
    host = urlparse(url).netloc
    for attempt in range(3):
        throttle(host, rate)
        try:
            resp = SESSION.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            raise GazetteerError(redact(f"Request to {host} failed: {e}")) from None
        if resp.status_code in (429, 503) and attempt < 2:
            time.sleep(2 ** attempt)
            continue
        if not resp.ok:
            raise GazetteerError(f"HTTP {resp.status_code} from {redact(resp.url)}")
        try:
            return resp.json()
        except ValueError:
            raise GazetteerError(f"No JSON in the response from {redact(resp.url)}") from None


# %% 3. Gazetteer lookups ----------------------------------------------------------
# Each lookup returns the names around a point, nearest first. One name is a
# dict (R: one row of a tibble) with: place_id, name_id, feature_type,
# distance_m, name, language, name_status, spelling_status.

def ssr_near(lat, lon, radius_m=2000, max_pages=4):
    """Kartverket SSR /punkt: places within radius_m (<= 5000) and all their names.

    Results are not sorted by distance on the server, so sort here.
    """
    places = []
    for side in range(1, max_pages + 1):
        res = api_get(SSR_API, "punkt", rate=2, nord=lat, ost=lon, koordsys=4258,
                      radius=round(float(radius_m)), utkoordsys=4258, treffPerSide=500,
                      side=side)
        places += res.get("navn") or []
        md = res.get("metadata") or {}
        if md.get("viserTil") is None or md["viserTil"] >= md.get("totaltAntallTreff", 0):
            break
    names = [{"place_id": first(p.get("stedsnummer")),
              "name_id": join_id(p.get("stedsnummer"), nm.get("stedsnavnnummer")),
              "feature_type": first(p.get("navneobjekttype")),
              "distance_m": num1(p.get("meterFraPunkt")),
              "name": nfc(first(nm.get("skrivemåte"))),
              "language": first(nm.get("språk")),
              "name_status": first(nm.get("navnestatus")),
              "spelling_status": first(nm.get("skrivemåtestatus"))}
             for p in places for nm in (p.get("stedsnavn") or [])]
    return sorted(names, key=lambda d: math.inf if d["distance_m"] is None else d["distance_m"])


def join_id(place, name):
    """SSR name ID '<stedsnummer>-<stedsnavnnummer>', None if a part is missing."""
    a, b = first(place), first(name)
    return None if a is None or b is None else f"{a}-{b}"


def pick(props, candidates):
    """First property that exists. R: pick_col()."""
    for c in candidates:
        if props.get(c) is not None:
            return first(props[c])
    return None


def point_lon_lat(coordinates):
    """lon, lat of a GeoJSON point (also copes with nested coordinates)."""
    c = coordinates
    while isinstance(c, list) and c and isinstance(c[0], list):
        c = c[0]
    if isinstance(c, list) and len(c) >= 2:
        return c[0], c[1]
    return None, None


def next_token(res):
    """Paging token from an OGC API 'next' link, None on the last page."""
    for link in res.get("links") or []:
        if link.get("rel") == "next" and link.get("href"):
            m = re.search(r"[?&]next=([^&]+)", link["href"])
            return unquote(m.group(1)) if m else None
    return None


def mml_near(lat, lon, radius_m=2000, n=1000, max_pages=4):
    """NLS Finland Geographic Names (OGC API Features): one feature per name.

    Needs MML_API_KEY in the environment. Property names are read flexibly
    (pick); names of one place share a placeId.
    """
    key = os.environ.get("MML_API_KEY", "")
    if not key:
        raise GazetteerError("MML_API_KEY is not set: NLS Finland lookups need a free API key.")
    bbox = bbox_for(lat, lon, radius_m)
    features, token = [], None
    for _ in range(max_pages):
        params = {"bbox": bbox, "limit": n, "api-key": key}
        if token:
            params["next"] = token
        res = api_get(MML_API, rate=2, **params)
        features += res.get("features") or []
        token = next_token(res)
        if not token:
            break
    names = []
    for f in features:
        props = f.get("properties") or {}
        f_lon, f_lat = point_lon_lat((f.get("geometry") or {}).get("coordinates"))
        if num1(f_lat) is None or num1(f_lon) is None:
            continue
        dist = haversine_m(lat, lon, float(f_lat), float(f_lon))
        if dist > radius_m:
            continue
        place_id = pick(props, ["placeId", "place_id"])
        names.append({
            "place_id": place_id.removeprefix("P_") if place_id else f"{f_lon} {f_lat}",
            "name_id": first(f.get("id")) or pick(props, ["placeNameId"]),
            "feature_type": pick(props, ["placeType", "placeTypeDescription"]),
            "distance_m": dist,
            "name": nfc(pick(props, ["spelling", "name"])),
            "language": pick(props, ["language", "languageCode"]),
            "name_status": pick(props, ["languageOfficiality", "languageOfficialStatus",
                                        "officialStatus", "languageDominance"]),
            "spelling_status": None,
        })
    return sorted(names, key=lambda d: d["distance_m"])


LOOKUPS = {"ssr": ssr_near, "nls": mml_near}


# %% 4. Matching -------------------------------------------------------------------

def names_in_text(text, names):
    """Gazetteer names (>= 4 characters) that occur as whole words in the text.

    One hit per distinct name, from the nearest place that has it. Each hit gets
    matched_text: the words as written in the specimen record.
    """
    if not names or not isinstance(text, str) or not text.strip():
        return []
    text = nfc(text)
    seen, hits = set(), []
    for d in names:                             # names are sorted by distance
        nm = d["name"]
        if not nm or len(nm) < 4 or nm in seen:
            continue
        seen.add(nm)
        m = re.search(rf"\b{re.escape(nm)}\b", text, flags=re.IGNORECASE)
        if m:
            hits.append({**d, "matched_text": m.group(0)})
    return hits


def is_indigenous(d):
    return language_info(d["language"])[2] == INDIGENOUS


def summarise_hits(names, text, exclude_types=EXCLUDE_TYPES):
    """Step-1 summary for one location: name match first, then proximity.

    names: gazetteer names around the point, nearest first (None if the
    lookup failed). text: the locality text.
    status: "match"   the text names a place that has an Indigenous name
            "nearby"  only Indigenous names near the point (weaker)
            "no_match", or "error" if the lookup failed
    """
    lookup_ok = names is not None
    names = [d for d in (names or [])
             if d["name"] and d["feature_type"] not in exclude_types
             and status_flag(d["name_status"]) != "rejected"]
    indig = [d for d in names if is_indigenous(d)]
    # name match: locality text -> gazetteer place -> its Indigenous names
    matched = names_in_text(text, names)
    matched_ids = {d["place_id"] for d in matched}
    parallel = [d for d in names if d["place_id"] in matched_ids and is_indigenous(d)]
    if not lookup_ok:
        status = "error"
    elif parallel:
        status = "match"
    elif indig:
        status = "nearby"
    else:
        status = "no_match"
    return {
        "status": status,
        "lookup_ok": lookup_ok,
        "n_names": len(names) if lookup_ok else None,
        "n_indigenous": len(indig) if lookup_ok else None,
        "nearest_indigenous_name": indig[0]["name"] if indig else None,
        "nearest_distance_m": round_m(indig[0]["distance_m"]) if indig else None,
        "matched_name": join_unique(d["name"] for d in matched),
        "indigenous_parallel": join_unique(d["name"] for d in parallel),
        "parallel_language": join_unique(d["language"] for d in parallel),
        "gazetteer_place_id": parallel[0]["place_id"] if parallel else None,
        "has_parallel": bool(parallel),
        "names": names,
        "indigenous": indig,
        "matched": matched,
        "parallel": parallel,
    }


def homonym_reach(unc, radius_m, nearest_m):
    """How far away another place with the matched name still competes: the
    coordinate uncertainty, at least the matched place's distance, at most the
    search radius. Unknown uncertainty: the whole search radius."""
    if unc is None:
        return radius_m
    return min(radius_m, max(unc, num1(nearest_m) or 0))


def ambiguity(matched_name, names, reach_m=math.inf):
    """Places within reach_m that carry the matched name, and how many different
    sets of Indigenous names those places have (1 = they all agree)."""
    key = nfc(matched_name).casefold()
    places = list(dict.fromkeys(d["place_id"] for d in names
                                if d["name"] and d["name"].casefold() == key
                                and (d["distance_m"] is None or d["distance_m"] <= reach_m)))
    sets = {tuple(sorted({d["name"] for d in names
                          if d["place_id"] == pid and is_indigenous(d)}))
            for pid in places}
    return len(places), len(sets)


# %% 5. Results ----------------------------------------------------------------------

def score_match(match_type, distance_m=None, radius_m=None, n_name_sets=1, name_status=None):
    """Confidence in [0, 1] for one name (-> ods:score).

    name:      0.9, divided by the number of different Indigenous-name sets
               among places that carry the matched name (ambiguity). Places
               farther away than the coordinate uncertainty don't count.
    proximity: 0.5 x (1 - distance / search radius)
    Historical names (SSR 'historisk') get half.
    """
    if match_type == "name":
        s = NAME_MATCH_SCORE / max(1, n_name_sets)
    else:
        d = num1(distance_m)
        s = 0.0 if d is None else PROXIMITY_SCORE * max(0.0, 1 - d / radius_m)
    if status_flag(name_status) == "historical":
        s = s / 2
    return round2(s)


def caveats_for(d, gaz, match_type, unc, radius_m, n_places=1, n_name_sets=1, matched=None,
                reach_m=None):
    """Plain-text caveats for one name (-> dwc:relationshipRemarks)."""
    out = [(f"Official name in a state gazetteer ({GAZETTEERS[gaz]['label']}), "
            "not community-sourced Indigenous knowledge.")]
    if status_flag(d["name_status"]) == "historical":
        out.append(f"Name status '{d['name_status']}': not authorised for current public use.")
    _, language_name, category = language_info(d["language"])
    if category == MINORITY:
        out.append(f"{language_name} is a national minority language, not an Indigenous "
                   "language: listed apart and not annotated as an Indigenous name.")
    if match_type == "proximity":
        out.append("Proximity only: no place named in the locality text has this name; "
                   f"it belongs to a place {fmt_m(d['distance_m'])} from the coordinates.")
    if unc is None:
        out.append(f"Coordinate uncertainty not recorded; searched {fmt_m(radius_m)} "
                   "around the coordinates.")
    elif unc > radius_m:
        out.append(f"Coordinate uncertainty ({fmt_m(unc)}) is larger than the search "
                   f"radius ({fmt_m(radius_m)}); places further away were not checked.")
    if matched is not None and n_places > 1:
        differ = "their Indigenous names differ; " if n_name_sets > 1 else ""
        out.append(f"{n_places} places named '{matched['name']}' within "
                   f"{fmt_m(reach_m if reach_m is not None else radius_m)}; "
                   f"{differ}the nearest is used.")
    if matched is not None and matched["name"].casefold() == d["name"].casefold():
        out.append("The locality text already uses this Indigenous name.")
    return out


def matched_on(m):
    """What the match is based on: a name in the locality text, or the coordinates."""
    if m is None:
        return {"field": "coordinates", "text": None,
                "gazetteer_name": None, "gazetteer_language": None}
    return {"field": "locality", "text": m["matched_text"], "gazetteer_name": m["name"],
            "gazetteer_language": language_info(m["language"])[0] or m["language"]}


def name_record(d, gaz, match_type, matched, score, caveats, today):
    """One output record for one gazetteer name."""
    iso, language_name, category = language_info(d["language"])
    return {
        "name": d["name"],
        "language": iso,
        "language_name": language_name,
        "language_category": category,
        "gazetteer_language": d["language"],
        "name_status": d["name_status"],
        "spelling_status": d["spelling_status"],
        "gazetteer": GAZETTEERS[gaz]["label"],
        "place_id": d["place_id"],
        "place_uri": place_uri(d["place_id"], gaz),
        "name_id": d["name_id"],
        "feature_type": d["feature_type"],
        "match_type": match_type,
        "matched_on": matched_on(matched),
        "distance_m": round_m(d["distance_m"]),
        "score": score,
        "attribution": attribution(gaz, today),
        "licence": GAZETTEERS[gaz]["licence"],
        "caveats": caveats,
    }


def to_matches(hits, gaz, unc, radius_m, today, max_nearby=MAX_NEARBY):
    """Output records for one summarised location (see summarise_hits).

    Returns (matches, minority_language_names, nearby):
      matches   Indigenous names of the places named in the locality text
      minority  Kven (national minority) names of those places, kept apart
      nearby    only if nothing matched: the nearest Indigenous names
    """
    by_place = {}
    for m in hits["matched"]:
        by_place.setdefault(m["place_id"], m)
    matches, minority = [], []
    for d in hits["names"]:                          # nearest first
        m = by_place.get(d["place_id"])
        category = language_info(d["language"])[2]
        if m is None or category == OTHER:
            continue
        reach = homonym_reach(unc, radius_m, m["distance_m"])
        n_places, n_sets = ambiguity(m["name"], hits["names"], reach)
        rec = name_record(
            d, gaz, "name", m,
            score_match("name", n_name_sets=n_sets, name_status=d["name_status"]),
            caveats_for(d, gaz, "name", unc, radius_m, n_places, n_sets, m, reach), today)
        (matches if category == INDIGENOUS else minority).append(rec)
    nearby = []
    if not matches:
        for d in hits["indigenous"]:
            if len(nearby) >= max_nearby:
                break
            if d["name"] in {r["name"] for r in nearby}:
                continue
            nearby.append(name_record(
                d, gaz, "proximity", None,
                score_match("proximity", d["distance_m"], radius_m, name_status=d["name_status"]),
                caveats_for(d, gaz, "proximity", unc, radius_m), today))
    return matches, minority, nearby


def message_for(status, label, radius_m, n_places=0, n_nearby=0):
    if status == "match":
        return (f"{n_places} place(s) named in the locality text have Indigenous-language "
                f"names in {label}.")
    if status == "nearby":
        return (f"No place named in the locality text has an Indigenous-language name in "
                f"{label}; {n_nearby} Indigenous-language name(s) within {fmt_m(radius_m)} "
                "of the coordinates.")
    return (f"No Indigenous-language names in {label} within {fmt_m(radius_m)} of the "
            "coordinates, and no name match on the locality text.")


def find_indigenous_names(location, gazetteer=None, max_nearby=MAX_NEARBY, today=None):
    """Indigenous-language place names for one specimen location.

    location: dict with locality, county, country (or country_code), latitude,
      longitude, coordinate_uncertainty_m; optional specimen_id, event_index
      (the keys of data/benchmark-indigenous.json).
    gazetteer: "ssr" or "nls"; default chosen from the country.
    Returns a dict: status, gazetteer, query, matches, minority_language_names,
    nearby, summary, message, retrieved. status is one of match, nearby,
    no_match, no_coordinates, not_covered, error.
    """
    today = today or datetime.now(UTC).date()
    out = {"specimen_id": location.get("specimen_id"),
           "event_index": location.get("event_index", 0),
           "status": None, "gazetteer": None, "query": None,
           "matches": [], "minority_language_names": [], "nearby": [],
           "summary": None, "message": None, "retrieved": today.isoformat()}
    gaz = gazetteer or gazetteer_for(location)
    if gaz is None:
        out.update(status="not_covered",
                   message=("No gazetteer with Indigenous-language names for this country "
                            "yet (covered: Norway, Finland)."))
        return out
    meta = GAZETTEERS[gaz]
    out["gazetteer"] = meta["label"]
    lat, lon = num1(location.get("latitude")), num1(location.get("longitude"))
    if lat is None or lon is None:
        out.update(status="no_coordinates",
                   message="No decimal coordinates: the gazetteer lookup needs a point.")
        return out
    unc = num1(location.get("coordinate_uncertainty_m"))
    radius = radius_for(unc, max_m=meta["max_radius_m"])
    out["query"] = {"url": query_url(gaz, lat, lon, radius), "latitude": lat,
                    "longitude": lon, "radius_m": round_m(radius),
                    "coordinate_uncertainty_m": unc}
    try:
        names = LOOKUPS[gaz](lat, lon, radius_m=radius)
    except (GazetteerError, requests.RequestException, KeyError, TypeError, ValueError) as e:
        out.update(status="error", message=redact(f"{meta['label']} lookup failed: {e}"))
        return out
    hits = summarise_hits(names, location.get("locality"))
    matches, minority, nearby = to_matches(hits, gaz, unc, radius, today, max_nearby)
    n_places = len({m["place_id"] for m in matches})
    out.update(status=hits["status"], matches=matches, minority_language_names=minority,
               nearby=nearby, summary={k: hits[k] for k in SUMMARY_FIELDS},
               message=message_for(hits["status"], meta["label"], radius, n_places,
                                   len(nearby)))
    return out
