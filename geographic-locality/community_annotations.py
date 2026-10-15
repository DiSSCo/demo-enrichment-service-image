"""Community comments: one oa:commenting per specimen location.

For each location the comment says whether a Sámi community may be connected to the
locality and, if so, that the community needs to be contacted for collaboration and
approval before Sámi knowledge is added to the record: the holding institution should
contact the Sámi bodies in the team's contact registry (a placeholder until the Sámi side
decides who speaks for whom). It never holds a name.

- every annotation is oa:commenting (never merged into the record, can be deleted);
- oa:value holds the comment lines from community_context (team repo), one per line,
  ending with "Built without Sámi input." and the review status;
- a failed name-register lookup with no listed area raises CommunityContextError: the
  job is marked FAILED and can be re-run.

Selectors: $['ods:hasEvents'][i]['ods:hasLocation'] for a location,
$['ods:hasEvents'] when the specimen has no location.
"""

from typing import Any

from geographic_locality_mas.community_context import REVIEW_STATUS

import shared

REVIEW_NOTE = f"Review status: {REVIEW_STATUS}."
BUILT_WITHOUT = "Built without Sámi input."
EVENTS_PATH = "$['ods:hasEvents']"


class CommunityContextError(RuntimeError):
    """The community context could not be assessed: the job is marked FAILED."""


def target_of(specimen: dict[str, Any]) -> dict[str, Any]:
    """Target id and type of a Digital Specimen: bare openDS or API wrapper."""
    attrs = specimen.get("attributes") or specimen
    return {
        "id": attrs.get(shared.ODS_ID) or attrs.get(shared.AT_ID) or specimen.get("id"),
        "type": attrs.get(shared.ODS_TYPE),
        "has_events": "ods:hasEvents" in attrs,
    }


def comment(
    agent: dict[str, Any], timestamp: str, lines: list[str], path: str, target: dict[str, Any], refs: str
) -> dict[str, Any]:
    """oa:commenting built with shared.py; oa:value holds one string per line."""
    out = shared.map_to_annotation_str_val(
        agent,
        timestamp,
        lines[0],
        shared.build_class_selector(path),
        target["id"],
        target["type"],
        refs,
        shared.Motivation.COMMENTING.value,
    )
    out["oa:hasBody"]["oa:value"] = list(lines)
    return out


def build_annotations(
    specimen: dict[str, Any],
    response: dict[str, Any],
    agent: dict[str, Any],
    timestamp: str,
) -> list[dict[str, Any]]:
    """One community comment per location; raises CommunityContextError if a lookup failed."""
    target = target_of(specimen)
    results = response.get("results") or []
    if not results:
        lines = ["No location in the specimen record: community context not assessed.", BUILT_WITHOUT, REVIEW_NOTE]
        return [comment(agent, timestamp, lines, EVENTS_PATH if target["has_events"] else "$", target, "")]
    annotations = []
    for result in results:
        cc = result["community_context"]
        if cc["status"] == "error":
            raise CommunityContextError(cc.get("message") or "Name register lookup failed.")
        refs = next((a["source_url"] for a in cc.get("areas") or [] if a.get("source_url")), "")
        annotations.append(comment(agent, timestamp, cc["comment"], result["target"], target, refs))
    return annotations
