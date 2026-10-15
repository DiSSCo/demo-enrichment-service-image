"""Community context for one specimen location: may a Sámi community be
connected to this locality, and whom should the holding institution contact?

DiSSCo MAS Hackathon 2026 - Geographic Place Names team.
Workstream: Indigenous place names, step 3: from "which Indigenous name?" to
"who should be involved?". Concept: docs/indigenous-community-context.md.

Python twin of R/community_context.R, which is kept outside this repo. Same
function names, same outputs.

For one specimen location, after the coordinate check if one ran (validation.py,
validate_coordinates_with_tolerance, branch feature/geographic-validation):
  1. Context: may a Sámi community be connected to this locality?
     Evidence: legally defined areas (data/indigenous-areas.csv) and the
     official name registers (find_indigenous_names(), counts only).
  2. Who acts: the holding institution should contact the Sámi bodies in
     data/community-contacts.csv (national, then local) for collaboration and
     approval before Sámi knowledge is added to the record.
The output never holds names. Place names, species names, uses, stories stay
with the community: the output only says such knowledge may exist and whom to ask.

The two CSV files are data, not code. The contact registry is a placeholder
until the Sámi side decides who speaks for whom; every row says so.
COMMUNITY_DATA_DIR overrides where the CSV files are read from.

Use (one call per location, as the MAS does):
    cc = assess_location(location, qc_status=None)   # qc_status: Craig's check, if run
    cc["status"], cc["comment"]                      # comment: the oa:value lines
"""

import csv
import math
import os
import re
import unicodedata
from pathlib import Path

REVIEW_STATUS = "not reviewed by Sámi representatives"

# Country -> registry key. Same patterns as COUNTRY_GAZETTEER in indigenous_names.py
COUNTRIES = [
    ("NO", r"^(no|nor)$|norway|norge|noreg|norga|norwegen"),
    ("FI", r"^(fi|fin)$|finland|suomi|suopma|finnland"),
]

# Results of the coordinate check that rule the coordinates out.
# near_boundary stays usable: the Vardø test specimen is 2.2 km outside the
# 1:10m coastline. unable_to_validate (e.g. country "NO") = not checked.
QC_BAD = ("inconsistent", "invalid_coordinates")

# Admin fields the area table is matched against (flat location keys). The free
# locality text is left out on purpose: Kåfjord or Tana also name other places.
ADMIN_FIELDS = {
    "municipality": ("municipality",),
    "county": ("county",),
    "stateProvince": ("stateProvince", "state_province"),
    "higherGeography": ("higherGeography", "higher_geography"),
}

CONTACT_LEVELS = ("national", "local")


# %% Helpers ------------------------------------------------------------------------

def loc_text(location, keys):
    """First non-empty value among keys, as trimmed text (None if none)."""
    for k in keys:
        v = location.get(k)
        if isinstance(v, list):
            v = v[0] if v else None
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        v = str(v).strip()
        if v:
            return v
    return None


def loc_num(x):
    """x as a finite number, None otherwise."""
    if isinstance(x, list):
        x = x[0] if x else None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def nfc(x):
    return unicodedata.normalize("NFC", x)


def whole_word(text, word):
    """Does `word` occur as a whole word in `text`? (case-insensitive, NFC)"""
    return re.search(rf"\b{re.escape(nfc(word))}\b", nfc(text), flags=re.IGNORECASE) is not None


def fmt_m(x):
    return f"{loc_num(x):.0f} m"


def data_dir():
    """COMMUNITY_DATA_DIR, else data/ next to this file (MAS), else the repo's data/."""
    if os.environ.get("COMMUNITY_DATA_DIR"):
        return Path(os.environ["COMMUNITY_DATA_DIR"])
    here = Path(__file__).resolve().parent
    return here / "data" if (here / "data").is_dir() else here.parents[1] / "data"


def load_csv(path):
    with open(path, encoding="utf-8", newline="") as fh:
        return [{k: (v if v != "" else None) for k, v in row.items()}
                for row in csv.DictReader(fh)]


def load_areas(path=None):
    rows = load_csv(path or data_dir() / "indigenous-areas.csv")
    for r in rows:
        r["partial"] = r["partial"] == "TRUE"
    return rows


def load_contacts(path=None):
    return load_csv(path or data_dir() / "community-contacts.csv")


# %% Steps ----------------------------------------------------------------------------

def country_for(location):
    """'NO', 'FI' or None, from country_code or country."""
    for field in ("country_code", "country"):
        value = loc_text(location, [field])
        if value is None:
            continue
        for key, pattern in COUNTRIES:
            if re.search(pattern, value, flags=re.IGNORECASE):
                return key
    return None


def coords_usable(location, qc_status=None):
    """Coordinates present and not ruled out by the coordinate check."""
    return (loc_num(location.get("latitude")) is not None
            and loc_num(location.get("longitude")) is not None
            and qc_status not in QC_BAD)


def match_areas(location, country, areas=None):
    """Every area whose name (or alias) occurs in an admin field. All overlaps are
    kept, never just one."""
    areas = load_areas() if areas is None else areas
    values = {f: loc_text(location, keys) for f, keys in ADMIN_FIELDS.items()}
    values = {f: v for f, v in values.items() if v is not None}
    hits = []
    for a in (a for a in areas if a["country"] == country):
        words = [a["name"]] + (a["aliases"].split("|") if a["aliases"] else [])
        for f, value in values.items():
            if any(whole_word(value, w) for w in words):
                hits.append({"area": a["area"], "level": a["level"], "unit": a["name"],
                             "partial": a["partial"], "note": a["note"],
                             "defined_by": a["defined_by"], "source_url": a["source_url"],
                             "matched_field": f})
                break
    return hits


def name_evidence(names_result):
    """Register evidence from find_indigenous_names(): status, counts, ISO codes.
    Never the names themselves."""
    if names_result is None:
        return None
    q = names_result.get("query")
    ev = {"source": names_result.get("gazetteer"),
          "status": names_result.get("status"),
          "radius_m": loc_num(q.get("radius_m")) if isinstance(q, dict) else None,
          "n_names": None,
          "languages": []}
    if ev["status"] in ("match", "nearby", "no_match"):
        ev["n_names"] = int((names_result.get("summary") or {}).get("n_indigenous") or 0)
        recs = (names_result.get("matches") or []) + (names_result.get("nearby") or [])
        ev["languages"] = sorted({r["language"] for r in recs if r.get("language")})
    return ev


def community_context(location, qc_status=None, names_result=None, areas=None,
                      contacts=None):
    """Community context for one location.

    names_result: output of find_indigenous_names(location), or None if it was
    not run. status: indigenous_context | no_known_context | not_assessed |
    not_covered | error
    """
    areas = load_areas() if areas is None else areas
    contacts = load_contacts() if contacts is None else contacts
    event_index = loc_num(location.get("event_index", 0))
    out = {"specimen_id": loc_text(location, ["specimen_id"]),
           "event_index": int(event_index) if event_index is not None else None,
           "status": None, "country": None, "people": None,
           "qc": {"status": qc_status, "coordinates_used": False},
           "areas": [], "evidence": None, "institution": None, "contacts": [],
           "notice": None, "message": None, "review_status": REVIEW_STATUS}
    country = country_for(location)
    if country is None:
        out.update(status="not_covered",
                   message=("Indigenous context not assessed: no area layer or contact "
                            "registry for this country yet (covered: Norway, Finland)."))
        return out
    reg = sorted((c for c in contacts if c["country"] == country),
                 key=lambda c: CONTACT_LEVELS.index(c["level"]))
    out["country"] = country
    out["people"] = reg[0]["people"] if reg else None
    out["qc"]["coordinates_used"] = coords_usable(location, qc_status)
    out["areas"] = match_areas(location, country, areas)
    ev = name_evidence(names_result) if out["qc"]["coordinates_used"] else None
    out["evidence"] = ev

    names_found = ev is not None and ev["status"] in ("match", "nearby")
    if out["areas"] or names_found:
        out["status"] = "indigenous_context"
    elif ev is not None and ev["status"] != "no_match":
        out["status"] = "error"            # register lookup failed, nothing else: re-run
    elif ev is None:
        out["status"] = "not_assessed"     # no usable coordinates, or lookup not run
    else:
        out["status"] = "no_known_context"

    if out["status"] == "indigenous_context":
        out["institution"] = {
            "name": loc_text(location, ["organisation", "organisation_name"]),
            "url": loc_text(location, ["organisation_id"]),
            "role": ("holds the specimen; contacts the bodies listed and is responsible "
                     "for the engagement"),
            "status": "from the specimen record"}
        out["contacts"] = [
            {k: c[k] for k in ("level", "contact", "role", "url", "status")} for c in reg]
        out["notice"] = ("Local Contexts BC Notice (candidate): the holding institution "
                         "can apply it through the Local Contexts Hub.")
    out["message"] = context_message(out)
    return out


def context_message(cc):
    people = cc["people"] or "Indigenous"
    qc = cc["qc"]["status"]
    why_not = (f"the coordinates failed the coordinate check ({qc})" if qc in QC_BAD
               else "there are no usable coordinates or the register was not checked")
    return {
        "indigenous_context": (f"A {people} community may be connected to this locality. "
                               f"Before {people} knowledge is added to this record, the "
                               "community needs to be contacted for collaboration and "
                               "approval."),
        "no_known_context": (f"No {people} area and no registered {people}-language name "
                             f"found. This does not mean there is no {people} connection."),
        "not_assessed": (f"{people} context not assessed: {why_not}, and no recorded admin "
                         f"unit lies in a listed {people} area."),
        "error": (f"Name register lookup failed ({(cc['evidence'] or {}).get('source')}) "
                  f"and no recorded admin unit lies in a listed {people} area: re-run later."),
    }.get(cc["status"], cc["message"])


# %% Annotation text -------------------------------------------------------------------

def community_comment(cc):
    """Lines for oa:hasBody / oa:value of one oa:commenting annotation on
    $['ods:hasEvents'][i]['ods:hasLocation']. No names, ever."""
    people = cc["people"] or "Indigenous"
    if cc["status"] != "indigenous_context":
        lines = [cc["message"]]
    else:
        lines = [cc["message"]]
        for a in cc["areas"]:
            part = f", partly: {a['note']}" if a["partial"] else ""
            lines.append(f"Within: {a['area']} ({a['level']} {a['unit']}{part}; "
                         f"{a['defined_by']}; matched on dwc:{a['matched_field']}).")
        if not cc["qc"]["coordinates_used"] and cc["qc"]["status"] in QC_BAD:
            lines.append(f"Coordinate check: {cc['qc']['status']}; coordinates not used, "
                         "context from the recorded admin units only.")
        ev = cc["evidence"]
        if ev is not None and ev["status"] in ("match", "nearby"):
            what = ("a place named in the locality text has" if ev["status"] == "match"
                    else "places near the coordinates have")
            lines.append(f"Official name register ({ev['source']}): {what} {people}-language "
                         f"names ({', '.join(ev['languages'])}; {ev['n_names']} within "
                         f"{fmt_m(ev['radius_m'])}). Names are not copied here.")
        inst = cc["institution"] or {}
        who = "The holding institution"
        if inst.get("name"):
            who += f", {inst['name']}" + (f" <{inst['url']}>" if inst.get("url") else "") + ","
        lines += [
            (f"{people} communities may hold knowledge about this place and this organism "
             "(e.g. names, uses, stories) and have rights in how this record and data from "
             "it are used. Add such knowledge only with their free, prior and informed "
             "consent."),
            f"{who} should contact, for collaboration and approval, in this order:"]
        for i, k in enumerate(cc["contacts"], start=1):
            lines.append(f"{i}. {k['contact']}"
                         + (f" <{k['url']}>" if k["url"] else "")
                         + (f": {k['role']}" if k["role"] else "")
                         + f" [{k['status']}].")
        lines.append(cc["notice"])
    return lines + ["Built without Sámi input.", f"Review status: {cc['review_status']}."]


def names_to_guard(names_result, location=None, register_names=None):
    """Sámi and Kven names from the lookup that a text must not hold: the names in
    names_result plus register_names (every name the lookup returned, see
    find_indigenous_names(register_names=...)). Names the record itself already
    uses (its own text fields) don't count: they are the institution's text."""
    recs = []
    if names_result:
        recs = ((names_result.get("matches") or [])
                + (names_result.get("minority_language_names") or [])
                + (names_result.get("nearby") or []))
    names = {r["name"] for r in recs if r.get("name")} | set(register_names or ())
    own = " ".join(str(v) for v in (location or {}).values() if isinstance(v, str))
    return sorted(n for n in names if n and not (own and whole_word(own, n)))


def assert_no_names(lines, names_result, location=None, register_names=None):
    """Raise if any name from the lookup appears in the text. The error never
    repeats the name. See names_to_guard() for location and register_names."""
    text = "\n".join(lines)
    if any(whole_word(text, nm) for nm in names_to_guard(names_result, location,
                                                          register_names)):
        raise ValueError("A gazetteer name appears in the annotation text: not published.")
    return True


# %% Coordinate check --------------------------------------------------------------------

def qc_status_for(location, tolerance_km=5.0):
    """Status from validate_coordinates_with_tolerance() in validation.py (Craig),
    or None where it is not available (not merged yet, no shapely/pyproj, or no
    reference file, e.g. in the MAS container)."""
    try:
        from .validation import validate_coordinates_with_tolerance
    except ImportError:
        return None
    flat = {"specimen_id": location.get("specimen_id"), "country": location.get("country"),
            "latitude": loc_num(location.get("latitude")),
            "longitude": loc_num(location.get("longitude"))}
    try:
        return validate_coordinates_with_tolerance(flat, tolerance_km=tolerance_km)["status"]
    except OSError:
        return None


# %% One call per location (MAS) ----------------------------------------------------------

def assess_location(location, qc_status=None, register_evidence=True):
    """Community context for one location, with its comment: the one call the MAS
    makes per location (in one integrated MAS: after the coordinate check).

    location: flat location (keys: docs/indigenous-community-context.md).
    qc_status: status from Craig's coordinate check if it ran; None = not checked.
    register_evidence: False = legal areas only, no name-register lookup.
    Register lookups only with usable coordinates; Finland only with MML_API_KEY.
    Returns community_context() plus "comment" (the oa:value lines). Names from
    the registers never leave this function: the names guard raises ValueError
    if the comment would hold one.
    """
    from .indigenous_names import find_indigenous_names, gazetteer_for

    names, register = None, set()
    no_key = gazetteer_for(location) == "nls" and not os.environ.get("MML_API_KEY")
    if register_evidence and coords_usable(location, qc_status) and not no_key:
        names = find_indigenous_names(location, register_names=register)
    cc = community_context(location, qc_status=qc_status, names_result=names)
    cc["comment"] = community_comment(cc)
    assert_no_names(cc["comment"], names, location, register)
    return cc
