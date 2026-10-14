"""Pointer-only annotations: comments that link to the gazetteer register entry.

Demo default for the DiSSCo Sandbox, where annotations are public and hard to
withdraw. Sámi place names are Sámi knowledge, even when they come from state
gazetteers, and this prototype is built without Sámi input. So:

- every annotation is oa:commenting (never merged into the record, can be deleted);
- the body holds links to register entries, ISO 639-3 language codes, name status,
  distances and confidence, never a place-name string;
- every body ends with the review status ("not reviewed by Sámi representatives");
- Kven names (a national minority language, not Indigenous) are never annotated;
- assert_no_names() checks every annotation against the Indigenous and Kven names
  in the value-service response and refuses to publish on a hit.

Selectors: $['ods:hasEvents'][i]['ods:hasLocation'] for a location,
$['ods:hasEvents'] when the specimen has no location.
"""

import json
import re
import unicodedata
from typing import Any

import indigenous_names as ina

import shared

PREFIX = "Indigenous place-name pointer (prototype)."
NO_NAMES = "This annotation links to the register and copies no names."
KNOWLEDGE = (
    "Official names from a state gazetteer; Sámi place names are still Sámi knowledge. Built without Sámi input."
)
REVIEW_NOTE = f"Review status: {ina.REVIEW_STATUS}."
MAX_PLACES = 3
EVENTS_PATH = "$['ods:hasEvents']"
NAME_LISTS = ("matches", "minority_language_names", "nearby")
NAME_SUMMARY_FIELDS = ("indigenous_parallel", "nearest_indigenous_name")


class IndigenousNamesError(RuntimeError):
    """The value service could not look up a location: the job is marked FAILED."""


class NamesInAnnotationError(RuntimeError):
    """An annotation would contain a gazetteer name. The message never holds the name."""


# ---- Text ----------------------------------------------------------------------------


def by_place(records: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Name records grouped per gazetteer place, in input order (nearest first)."""
    groups: dict[Any, list[dict[str, Any]]] = {}
    for r in records:
        groups.setdefault((r.get("gazetteer"), r.get("place_id")), []).append(r)
    return list(groups.values())


def language_label(r: dict[str, Any]) -> str:
    """'Northern Sámi, sme, hovednavn': language and status, no name."""
    parts = [r.get("language_name"), r.get("language"), r.get("name_status")]
    return ", ".join(str(p) for p in parts if p)


def place_pointer(place: list[dict[str, Any]], proximity: bool = False) -> str:
    """One register entry: link, languages, then distance or confidence."""
    first = place[0]
    link = first.get("place_uri") or f"{first.get('gazetteer')} place {first.get('place_id')}"
    languages = "; ".join(dict.fromkeys(language_label(r) for r in place))
    if proximity:
        extra = f"{first.get('distance_m')} m from the coordinates"
    else:
        extra = f"confidence {max(r.get('score') or 0 for r in place)}"
    return f"{link} ({languages}; {extra})"


def radius_text(ind: dict[str, Any]) -> str:
    radius = (ind.get("query") or {}).get("radius_m")
    return f"within {radius} m of the coordinates" if radius is not None else "near the coordinates"


def body_text(ind: dict[str, Any]) -> str:
    """oa:value for one location result. Uses links, codes and numbers only."""
    gazetteer = ind.get("gazetteer") or "the gazetteer"
    status = ind.get("status")
    places = by_place(ind.get("matches") or [])
    nearby = by_place(ind.get("nearby") or [])
    parts = [PREFIX]
    if status == "match" and places:
        parts.append(
            f"The locality text names {len(places)} place(s) with Indigenous-language names in "
            f"{gazetteer}. Register entries: "
            + "; ".join(place_pointer(p) for p in places[:MAX_PLACES])
            + f". {NO_NAMES}"
        )
    elif status == "nearby" and nearby:
        parts.append(
            f"No place named in the locality text has an Indigenous-language name in {gazetteer}. "
            f"Register entries with Indigenous-language names {radius_text(ind)} (proximity only, "
            "not necessarily the collecting site): "
            + "; ".join(place_pointer(p, proximity=True) for p in nearby[:MAX_PLACES])
            + f". {NO_NAMES}"
        )
    else:
        parts.append(ind.get("message") or "No result.")
    if places or nearby:
        parts.append(KNOWLEDGE)
        attribution = (places or nearby)[0][0].get("attribution")
        if attribution:
            parts.append(attribution + ".")
    parts.append(REVIEW_NOTE)
    return " ".join(parts)


def references(ind: dict[str, Any]) -> str:
    """dcterms:references: the first register entry, else the gazetteer query (no key)."""
    for key in ("matches", "nearby"):
        for r in ind.get(key) or []:
            if r.get("place_uri"):
                return r["place_uri"]
    return (ind.get("query") or {}).get("url") or ""


def best_score(ind: dict[str, Any]) -> float | None:
    key = "matches" if ind.get("matches") else "nearby"
    scores = [r["score"] for r in ind.get(key) or [] if r.get("score") is not None]
    return max(scores) if scores else None


# ---- Annotations ---------------------------------------------------------------------


def target_of(specimen: dict[str, Any]) -> dict[str, Any]:
    """Target id and type of a Digital Specimen: bare openDS or API wrapper."""
    attrs = specimen.get("attributes") or specimen
    return {
        "id": attrs.get(shared.ODS_ID) or attrs.get(shared.AT_ID) or specimen.get("id"),
        "type": attrs.get(shared.ODS_TYPE),
        "has_events": "ods:hasEvents" in attrs,
    }


def comment(
    agent: dict[str, Any],
    timestamp: str,
    text: str,
    path: str,
    target: dict[str, Any],
    refs: str,
    score: float | None = None,
) -> dict[str, Any]:
    out = shared.map_to_annotation_str_val(
        agent,
        timestamp,
        text,
        shared.build_class_selector(path),
        target["id"],
        target["type"],
        refs,
        shared.Motivation.COMMENTING.value,
    )
    if score is not None:
        out["oa:hasBody"]["ods:score"] = score
    return out


def build_annotations(
    specimen: dict[str, Any],
    response: dict[str, Any],
    agent: dict[str, Any],
    timestamp: str,
) -> list[dict[str, Any]]:
    """One pointer comment per location; raises IndigenousNamesError if a lookup failed."""
    target = target_of(specimen)
    results = response.get("results") or []
    if not results:
        text = (
            f"{PREFIX} No location in the specimen record: nothing to match against "
            f"Indigenous place names. {REVIEW_NOTE}"
        )
        path = EVENTS_PATH if target["has_events"] else "$"
        return [comment(agent, timestamp, text, path, target, "")]
    annotations = []
    for result in results:
        ind = result["indigenous_names"]
        if ind.get("status") == "error":
            raise IndigenousNamesError(ind.get("message") or "Gazetteer lookup failed.")
        annotations.append(
            comment(
                agent,
                timestamp,
                body_text(ind),
                result.get("target") or f"$['ods:hasEvents'][{result['event_index']}]['ods:hasLocation']",
                target,
                references(ind),
                best_score(ind),
            )
        )
    return annotations


# ---- Guard ---------------------------------------------------------------------------


def nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def names_in_response(response: dict[str, Any]) -> list[str]:
    """Indigenous and Kven name strings in a value-service response."""
    names = set()
    for result in response.get("results") or []:
        ind = result.get("indigenous_names") or {}
        for key in NAME_LISTS:
            names.update(r.get("name") for r in ind.get(key) or [])
        summary = ind.get("summary") or {}
        for key in NAME_SUMMARY_FIELDS:
            names.update((summary.get(key) or "").split(" | "))
    return sorted(nfc(n) for n in names if isinstance(n, str) and len(n.strip()) >= 2)


def assert_no_names(annotations: list[dict[str, Any]], response: dict[str, Any]) -> None:
    """Raise NamesInAnnotationError if any annotation contains a name from the response.

    Whole-word, case-sensitive match on the serialised annotation (NFC).
    """
    names = names_in_response(response)
    for annotation in annotations:
        text = nfc(json.dumps(annotation, ensure_ascii=False))
        for name in names:
            if re.search(rf"(?<!\w){re.escape(name)}(?!\w)", text):
                raise NamesInAnnotationError(
                    "Refusing to publish: an annotation would contain a gazetteer place name (pointer-only mode)."
                )
