"""Normalize alternative country names for geographic validation.

Original specimen values are preserved. Normalization only supplies a
reference-country name for polygon lookup.
"""

COUNTRY_ALIASES = {
    "be": "Belgium",
    "russian federation": "Russia",
    "congo, democratic republic of the": "Democratic Republic of the Congo",
    "dr congo": "Democratic Republic of the Congo",
    "r.d. congo": "Democratic Republic of the Congo",
    "españa": "Spain",
    "united states": "United States of America",
    "usa": "United States of America",
    "tanzania": "United Republic of Tanzania",
    "tanzania, united republic of": "United Republic of Tanzania",
    "iran, islamic republic of": "Iran",
    "république du congo": "Republic of the Congo",
    "grecia": "Greece",
    "côte d'ivoire": "Ivory Coast",
    "cape verde": "Cabo Verde",
    "the netherlands": "Netherlands",
    "türkiye": "Turkey",
    "francia": "France",
    "czech republic": "Czechia",
    "italia": "Italy",
    "macedonia": "North Macedonia",
    "timor-leste": "East Timor",
    "nederland": "Netherlands",
    "republic of indonesia": "Indonesia",
    "republic of congo": "Republic of the Congo",
}


def normalize_country(country):
    """Return the reference-country name, retaining unknown values unchanged."""
    if not isinstance(country, str):
        return country

    cleaned = country.strip()
    return COUNTRY_ALIASES.get(cleaned.casefold(), cleaned)
