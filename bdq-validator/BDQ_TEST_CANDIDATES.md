# BDQ tests that need no external API

Which [BDQ TG2 tests](https://github.com/gbif-norway/bdq-multirecord-agent/blob/main/bdq-api/TG2_tests.csv)
can be implemented in pure Python in this DiSSCo MAS, without calling external
services?

Of the 110 tests in `TG2_tests.csv`, about 60 need no API. The rest depend on the
GBIF Backbone, Getty TGN, Natural Earth/EEZ shapefiles, WoRMS or EPSG.

Sources:
[BDQ user guide](https://bdq.tdwg.org/draft/docs/guide/users/index.html),
[TG2_tests.csv](https://github.com/gbif-norway/bdq-multirecord-agent/blob/main/bdq-api/TG2_tests.csv),
[bdq-multirecord-agent](https://github.com/gbif-norway/bdq-multirecord-agent).

## Tier 1: presence checks (trivial)

Same pattern as the existing `ValidationEventDateNotEmpty`, reusing `is_empty`
from `bdq.py` with a different term.

| Area | Tests |
| --- | --- |
| Event | `VALIDATION_YEAR_NOTEMPTY`, `VALIDATION_EVENTTEMPORAL_NOTEMPTY` (any of eventDate/year/month/day/startDayOfYear/endDayOfYear/verbatimEventDate) |
| Location | `VALIDATION_DECIMALLATITUDE_NOTEMPTY`, `VALIDATION_DECIMALLONGITUDE_NOTEMPTY`, `VALIDATION_COUNTRY_NOTEMPTY`, `VALIDATION_COUNTRYCODE_NOTEMPTY`, `VALIDATION_GEODETICDATUM_NOTEMPTY`, `VALIDATION_LOCATION_NOTEMPTY` |
| Taxon | `VALIDATION_SCIENTIFICNAME_NOTEMPTY`, `VALIDATION_KINGDOM_NOTEMPTY`, `VALIDATION_TAXONRANK_NOTEMPTY`, `VALIDATION_SCIENTIFICNAMEID_NOTEMPTY`, `VALIDATION_SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY`, `VALIDATION_NAMEPUBLISHEDINYEAR_NOTEMPTY`, `VALIDATION_TAXON_NOTEMPTY` |
| Record | `VALIDATION_BASISOFRECORD_NOTEMPTY`, `VALIDATION_OCCURRENCEID_NOTEMPTY`, `VALIDATION_LICENSE_NOTEMPTY`, `VALIDATION_DCTYPE_NOTEMPTY`, `VALIDATION_OCCURRENCESTATUS_NOTEMPTY` |
| Issue | `ISSUE_DATAGENERALIZATIONS_NOTEMPTY`, `ISSUE_ESTABLISHMENTMEANS_NOTEMPTY` (these flag the *presence* of a value) |

## Tier 2: numeric ranges and simple logic (easy)

| Test | Rule (defaults from the spec) |
| --- | --- |
| `VALIDATION_DECIMALLATITUDE_INRANGE` | −90 to 90 |
| `VALIDATION_DECIMALLONGITUDE_INRANGE` | −180 to 180 |
| `VALIDATION_COORDINATES_NOTZERO` | lat and long not both 0 |
| `VALIDATION_COORDINATEUNCERTAINTY_INRANGE` | numeric range check |
| `VALIDATION_MINELEVATION_INRANGE`, `VALIDATION_MAXELEVATION_INRANGE` | −430 to 8850 m |
| `VALIDATION_MINELEVATION_LESSTHAN_MAXELEVATION` | min ≤ max |
| `VALIDATION_MINDEPTH_INRANGE`, `VALIDATION_MAXDEPTH_INRANGE` | 0 to 11000 m |
| `VALIDATION_MINDEPTH_LESSTHAN_MAXDEPTH` | min ≤ max |
| `VALIDATION_YEAR_INRANGE` | 1582 to current year |
| `VALIDATION_MONTH_STANDARD` | integer 1–12 |
| `VALIDATION_DAY_STANDARD` | integer 1–31 |
| `VALIDATION_DAY_INRANGE` | valid for the given month/year (`calendar.monthrange`) |
| `AMENDMENT_OCCURRENCESTATUS_ASSUMEDDEFAULT` | fill in default `present` |
| `AMENDMENT_GEODETICDATUM_ASSUMEDDEFAULT` | fill in default `EPSG:4326` |

## Tier 3: date logic (moderate, no API)

Needs one ISO 8601 helper that handles partial dates and ranges, such as `1916-07`
and `1916-07-01/1916-07-15`. Python's `datetime` does not handle these by itself.
Write the helper once and reuse it for:

- `VALIDATION_EVENTDATE_STANDARD`
- `VALIDATION_EVENTDATE_INRANGE` (1582-11-15 to current year)
- `VALIDATION_DATEIDENTIFIED_STANDARD`
- `VALIDATION_DATEIDENTIFIED_INRANGE` (1753-01-01 to today)
- `VALIDATION_EVENT_CONSISTENT`
- `VALIDATION_STARTDAYOFYEAR_INRANGE`, `VALIDATION_ENDDAYOFYEAR_INRANGE`
- `MEASURE_EVENTDATE_DURATIONINSECONDS`
- `AMENDMENT_EVENT_FROM_EVENTDATE`, `AMENDMENT_EVENTDATE_FROM_YEARMONTHDAY`

The two amendments suit DiSSCo's annotation model well because they propose
real values, not just flags.

## Tier 4: small vocabularies bundled offline

The spec names an authority for each of these, but the authority is a short
fixed list or a regex that can ship in the code.

| Test | Authority to bundle |
| --- | --- |
| `VALIDATION_BASISOFRECORD_STANDARD` | Darwin Core basisOfRecord values |
| `VALIDATION_OCCURRENCESTATUS_STANDARD` | the spec's present/absent regex |
| `VALIDATION_DCTYPE_STANDARD` | DCMI Type Vocabulary (12 terms) |
| `VALIDATION_LICENSE_STANDARD` | Creative Commons regex |
| `VALIDATION_COUNTRYCODE_STANDARD` | ISO 3166-1 alpha-2 list (or `pycountry`) |
| `VALIDATION_ESTABLISHMENTMEANS_STANDARD`, `VALIDATION_DEGREEOFESTABLISHMENT_STANDARD`, `VALIDATION_PATHWAY_STANDARD` | Darwin Core controlled vocabularies |
| `VALIDATION_SEX_STANDARD`, `VALIDATION_TYPESTATUS_STANDARD`, `VALIDATION_TAXONRANK_STANDARD` | GBIF vocabularies (snapshot once) |

The matching `AMENDMENT_*_STANDARDIZED` tests can be added with synonym maps.
Record the snapshot date in `Response.comment` so results are reproducible.

## Tier 5: External services needed

| Dependency | Tests | Status |
| --- | --- | --- |
| GBIF Backbone | all `*_FOUND` taxon tests (`KINGDOM`, `PHYLUM`, `CLASS`, `ORDER`, `FAMILY`, `GENUS`, `SCIENTIFICNAME`), `VALIDATION_TAXON_UNAMBIGUOUS`, `VALIDATION_CLASSIFICATION_CONSISTENT`, `AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID`, `AMENDMENT_SCIENTIFICNAMEID_FROM_TAXON` | ✅ Implemented |
| Getty TGN | `VALIDATION_COUNTRY_FOUND`, `VALIDATION_COUNTRYCOUNTRYCODE_CONSISTENT`, `VALIDATION_STATEPROVINCE_FOUND`, `VALIDATION_COUNTRYSTATEPROVINCE_UNAMBIGUOUS` | Planned |
| Spatial shapefiles | `VALIDATION_COORDINATESCOUNTRYCODE_CONSISTENT`, `VALIDATION_COORDINATESSTATEPROVINCE_CONSISTENT`, `AMENDMENT_COORDINATES_TRANSPOSED`, `AMENDMENT_COUNTRYCODE_FROM_COORDINATES`, `ISSUE_COORDINATES_CENTEROFCOUNTRY` | Planned |
| WoRMS | `VALIDATION_COORDINATESTERRESTRIALMARINE_CONSISTENT` | Planned |
| EPSG | `VALIDATION_GEODETICDATUM_STANDARD`, `AMENDMENT_GEODETICDATUM_STANDARDIZED` | Planned |

These need no API but are fiddly to get right, so they are also out of scope:

- verbatim parsing: `AMENDMENT_COORDINATES_FROM_VERBATIM`, `AMENDMENT_EVENTDATE_FROM_VERBATIM`, `AMENDMENT_MINDEPTHMAXDEPTH_FROM_VERBATIM`, `AMENDMENT_MINELEVATIONMAXELEVATION_FROM_VERBATIM`
- name parsing: `VALIDATION_POLYNOMIAL_CONSISTENT`

## Recommended plan

1. **Generalise `BDQEvent`.** Today it is hard-wired to one term (`term`,
   `validate(value)`). Let each test declare its acted-upon terms and an OpenDS
   scope, and have `validate()` take a dict of terms. OpenDS keeps terms at
   different levels:

   | OpenDS scope | Example terms |
   | --- | --- |
   | `ods:DigitalSpecimen` root | basisOfRecord, license |
   | `ods:hasEvents[*]` | eventDate, year, month |
   | `ods:hasEvents[*].ods:hasLocation` | country, countryCode, locality |
   | `…ods:hasLocation.ods:hasGeoreference` | decimalLatitude, decimalLongitude, coordinateUncertaintyInMeters |
   | `ods:hasIdentifications[*].ods:hasTaxonIdentifications[*]` | scientificName, kingdom, family, taxonRank |

   One extractor per scope covers almost every test above. Check where
   `dc:type` and `occurrenceStatus` live in OpenDS: the sandbox specimen has
   `dcterms:type`, not `dc:type`, and has no `occurrenceStatus`.

2. **Generate test metadata from the CSV.** Load the label, version IRI and
   acted-upon terms from `TG2_tests.csv` instead of hard-coding them.

3. **Test against the official data.** TDWG publishes per-test validation data
   (input values with expected status, result and comment) in the `tdwg/bdq`
   repository. Parametrise the unit tests over it for conformance checking.

4. **Hackathon target.** Tier 1, Tier 2, plus `VALIDATION_EVENTDATE_STANDARD`,
   `VALIDATION_EVENTDATE_INRANGE` and `AMENDMENT_EVENT_FROM_EVENTDATE`. That is
   about 30 tests with no network calls. Then add
   `MEASURE_VALIDATIONTESTS_COMPLIANT`, `MEASURE_VALIDATIONTESTS_NOTCOMPLIANT` and
   `MEASURE_VALIDATIONTESTS_PREREQUISITESNOTMET` as one summary annotation per
   specimen. They only count the results of the other validations, so they come
   almost for free.
