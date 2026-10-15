# New BDQ tests: how they work

We added **16 new data quality tests** to the bdq-validator MAS. Together with the original
test (`VALIDATION_EVENTDATE_NOTEMPTY`), the MAS now runs **17 tests**, all of them by
default. None of them calls an external API.

## 1. Three possible answers

Every test gives one of three answers:

| Answer | Meaning |
| --- | --- |
| ✅ `COMPLIANT` | The value is good. |
| ❌ `NOT_COMPLIANT` | The value is there, but it is wrong. |
| ⚪ `INTERNAL_PREREQUISITES_NOT_MET` | The test **could not run** because the value is missing or unreadable. |

In the annotation, ✅ and ❌ come with the status `RUN_HAS_RESULT`. For ⚪, the
status is `INTERNAL_PREREQUISITES_NOT_MET` and the result is left empty.

## 2. Shared rules used by all tests

### When is a value "empty"?
- The term is missing, or
- it is text containing only spaces, tabs or newlines (`""`, `"   "`).

`"NULL"`, `"unknown"` and `"?"` are **not** empty: they count as values (which may
then be wrong).

### When is a value "a number"?
| Value | Number? |
| --- | --- |
| `125`, `125.5`, `-430` | yes |
| `"125"`, `"8850.1"`, `"1e3"` | yes (text that looks like a number) |
| `"high"`, `"12 m"`, `true`, `NaN`, infinity | no |

### When is a value "a whole number" (for the year)?
| Value | Whole number? |
| --- | --- |
| `1916`, `"1916"`, `1916.0` | yes |
| `1916.5`, `"19x6"`, `true` | no |

### When is a value "a valid date" (ISO 8601)?
A date is read as **the range of days it covers**:

| Value | Covers | Valid? |
| --- | --- | --- |
| `1916` | 1 Jan to 31 Dec 1916 | ✅ |
| `1916-07` | 1 to 31 July 1916 | ✅ |
| `1916-07-15` or `19160715` | 15 July 1916 | ✅ |
| `2001-032` (day 32 of the year) | 1 Feb 2001 | ✅ |
| `1963-03-08T14:07-0600` (with time) | 8 Mar 1963 | ✅ |
| `2007-11-13/2007-11-15` (a range) | 13 to 15 Nov 2007 | ✅ |
| `2007-11-13/15` (shortened range) | 13 to 15 Nov 2007 | ✅ |
| `1900/1909` | 1900 to 1909 | ✅ |
| `2001-02-29` | 2001 is not a leap year | ❌ |
| `1916-13`, `1916-7`, `07/1916`, `July 1916` | wrong format | ❌ |
| `2000-01-05/2000-01-04` | the range ends before it starts | ❌ |
| `" 1916"` | spaces around the date | ❌ |
| `2009-W01` (week number) | not supported (known limitation) | ❌ |

## 3. The tests, one by one

### Event tests: read from `ods:hasEvents[i]`

**VALIDATION_EVENTDATE_STANDARD**: is `dwc:eventDate` a real date?
1. Empty → ⚪
2. Valid date → ✅
3. Anything else → ❌

**VALIDATION_EVENTDATE_INRANGE**: is `dwc:eventDate` between **1582-11-15** and **31 Dec of this year**?
1. Empty → ⚪
2. Not a valid date → ⚪
3. The **whole** date range is inside the limits → ✅
4. Otherwise → ❌

Examples: `1916-07` ✅, `1582` ❌ (it starts before 15 Nov 1582), `2026-12-30/2027-01-02` ❌.

**VALIDATION_YEAR_INRANGE**: is `dwc:year` between **1582** and **this year**?
1. Empty → ⚪
2. Not a whole number → ⚪
3. Between 1582 and this year (both included) → ✅
4. Otherwise → ❌

### Location tests: read from `ods:hasEvents[i].ods:hasLocation`

**VALIDATION_MINELEVATION_INRANGE** (`dwc:minimumElevationInMeters`) and
**VALIDATION_MAXELEVATION_INRANGE** (`dwc:maximumElevationInMeters`): is the elevation plausible on Earth?
1. Empty → ⚪
2. Not a number → ⚪
3. Between **-430 m** (Dead Sea) and **8850 m** (Everest), both included → ✅
4. Otherwise → ❌

**VALIDATION_COUNTRYCODE_STANDARD**: is `dwc:countryCode` a real 2-letter country code?
1. Empty → ⚪
2. **Exactly** one of the 249 ISO codes, or `XZ` (high seas) or `ZZ` (unknown) → ✅
3. Otherwise → ❌

Examples: `BE` ✅, `be` ❌ (lowercase), `" BE"` ❌ (spaces), `BEL` ❌ (3 letters), `UK` ❌ (the ISO code is `GB`).

### Georeference test: read from `ods:hasEvents[i].ods:hasLocation.ods:hasGeoreference`

**VALIDATION_COORDINATES_NOTZERO**: are the coordinates something other than `0, 0`?
1. Latitude or longitude empty → ⚪
2. Latitude or longitude not a number → ⚪
3. At least one of them is not 0 → ✅
4. Both are 0 → ❌ (0, 0 is in the ocean and is usually a placeholder)

### Identification test: read from `ods:hasIdentifications[i]`

**VALIDATION_DATEIDENTIFIED_INRANGE**: is `dwc:dateIdentified` plausible?
It also looks at `dwc:eventDate` of the **first event** (the collecting event).

1. `dwc:dateIdentified` empty → ⚪
2. `dwc:dateIdentified` not a valid date → ⚪
3. `dwc:eventDate` filled in but not a valid date → ⚪
4. `dwc:dateIdentified` is not between **1753-01-01** and **today** → ❌
5. `dwc:dateIdentified` is **before** the collecting date → ❌ (you can't identify a specimen before collecting it)
6. Otherwise → ✅

Examples, with eventDate `1916-07`:

| dateIdentified | Answer | Why |
| --- | --- | --- |
| `1920-05-01` | ✅ | after collecting |
| `1916-07-15` | ✅ | during the collecting month |
| `1916-06-30` | ❌ | before collecting |
| `1752-12-31` | ❌ | before 1753 |
| tomorrow | ❌ | in the future |

If there is no eventDate, only steps 1, 2 and 4 apply.

### Specimen test: read from the specimen root

**VALIDATION_BASISOFRECORD_STANDARD**: is `dwc:basisOfRecord` an allowed value?
1. Empty → ⚪
2. **Exactly** one of `PreservedSpecimen`, `FossilSpecimen`, `LivingSpecimen`,
   `HumanObservation`, `MachineObservation`, `MaterialSample`, `Occurrence` → ✅
3. Otherwise → ❌ (for example `preservedspecimen` or `Specimen`)

### Taxon tests: read from `ods:hasIdentifications[i].ods:hasTaxonIdentifications[j]`

These only check that a value is **filled in**. Unlike the tests above, an empty value
gives ❌ (not ⚪): these tests always run.

**Six single-term tests**, all with the same logic:

| Test | Term |
| --- | --- |
| VALIDATION_SCIENTIFICNAME_NOTEMPTY | `dwc:scientificName` |
| VALIDATION_KINGDOM_NOTEMPTY | `dwc:kingdom` |
| VALIDATION_TAXONRANK_NOTEMPTY | `dwc:taxonRank` |
| VALIDATION_SCIENTIFICNAMEID_NOTEMPTY | `dwc:scientificNameID` |
| VALIDATION_SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY | `dwc:scientificNameAuthorship` |
| VALIDATION_NAMEPUBLISHEDINYEAR_NOTEMPTY | `dwc:namePublishedInYear` |

1. Empty → ❌
2. Anything else → ✅ (even `"unknown"`: these tests only check that something is there)

**VALIDATION_TAXON_NOTEMPTY**: is there *any* information about which taxon this is?
It looks at 25 terms: `taxonID`, `scientificNameID`, `acceptedNameUsageID`,
`parentNameUsageID`, `originalNameUsageID`, `taxonConceptID`, `scientificName`,
`higherClassification`, `kingdom`, `phylum`, `class`, `order`, `superfamily`, `family`,
`subfamily`, `tribe`, `subtribe`, `genus`, `genericName`, `subgenus`,
`infragenericEpithet`, `specificEpithet`, `infraspecificEpithet`, `vernacularName`,
`cultivarEpithet`.

1. At least one of them has a value → ✅ (the comment lists which ones)
2. All empty → ❌

`taxonRank` and `scientificNameAuthorship` are **not** in the list: a rank or an author
alone does not tell you the taxon. Four of the 25 terms (`parentNameUsageID`,
`originalNameUsageID`, `taxonConceptID`, `higherClassification`) do not exist in
OpenDS, so they are always empty.

## 4. How the MAS finds the values in a specimen

A DiSSCo specimen (OpenDS) is nested, so each test knows **where** its terms live:

```text
Digital Specimen                         ← basisOfRecord
├── ods:hasEvents[0]                     ← eventDate, year
│   └── ods:hasLocation                  ← countryCode, elevations
│       └── ods:hasGeoreference          ← decimalLatitude, decimalLongitude
└── ods:hasIdentifications[0]            ← dateIdentified
    └── ods:hasTaxonIdentifications[0]   ← scientificName, kingdom, taxonRank, ...
```

- A test runs **once per place**. If a specimen has 2 events, the event tests run
  twice and produce 2 annotations. If it has 2 identifications with 1 taxon each, the
  taxon tests also run twice.
- Every result becomes one annotation, including ⚪ results.

### What the annotation points at (the "selector")

| Situation | The annotation points at |
| --- | --- |
| The term exists | the term itself, e.g. `$['ods:hasEvents'][0]['ods:hasLocation']['dwc:countryCode']` |
| The term exists but is blank (`" "`) | the term itself (it exists, so it can be selected) |
| The term is missing | the closest object that exists, e.g. `$['ods:hasEvents'][0]['ods:hasLocation']` |
| Nothing exists (no events, no identifications) | the whole specimen |
| Test reads several terms (coordinates, taxon) | the object holding them, e.g. the georeference |

## 5. How to choose which tests run

By default, **all 17 tests run**. Use `--tests` to run only some of them.

```sh
# All 17 tests (default)
python3 bdq-validator/main.py --specimen SANDBOX/ZJZ-NMY-GN8

# Only some tests
python3 bdq-validator/main.py --specimen SANDBOX/ZJZ-NMY-GN8 \
  --tests VALIDATION_COUNTRYCODE_STANDARD,VALIDATION_COORDINATES_NOTZERO

# RabbitMQ / Docker: all tests by default, or choose with an environment variable
BDQ_TESTS=VALIDATION_EVENTDATE_NOTEMPTY python3 bdq-validator/main.py --rabbitmq

# Run the unit tests
python3 -m unittest discover -s bdq-validator -p 'test_*.py' -v
```

A misspelled test name stops the program immediately with a list of valid names.

## 6. Result on the real sandbox specimen (SANDBOX/ZJZ-NMY-GN8)

| Test | Answer | Value checked |
| --- | --- | --- |
| EVENTDATE_NOTEMPTY | ✅ | `1916-07` |
| EVENTDATE_STANDARD | ✅ | `1916-07` |
| EVENTDATE_INRANGE | ✅ | `1916-07` |
| YEAR_INRANGE | ✅ | `1916` |
| MINELEVATION_INRANGE | ⚪ | no elevation in this specimen |
| MAXELEVATION_INRANGE | ⚪ | no elevation in this specimen |
| COUNTRYCODE_STANDARD | ✅ | `BE` |
| COORDINATES_NOTZERO | ✅ | `51.026111, 4.478055` |
| DATEIDENTIFIED_INRANGE | ⚪ | no dateIdentified in this specimen |
| BASISOFRECORD_STANDARD | ✅ | `PreservedSpecimen` |
| SCIENTIFICNAME_NOTEMPTY | ✅ | `Chamaecyparis pisifera (Siebold & Zucc.) Endl.` |
| KINGDOM_NOTEMPTY | ✅ | `Plantae` |
| TAXONRANK_NOTEMPTY | ✅ | `SPECIES` |
| SCIENTIFICNAMEID_NOTEMPTY | ❌ | missing in this specimen |
| SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY | ✅ | `(Siebold & Zucc.) Endl.` |
| NAMEPUBLISHEDINYEAR_NOTEMPTY | ❌ | missing in this specimen |
| TAXON_NOTEMPTY | ✅ | 10 taxon terms have a value (taxonID, scientificName, kingdom, ...) |

⚪ is the correct answer for elevation and dateIdentified: the data is simply not in
the specimen. The two ❌ answers are correct too: a NOTEMPTY test reports missing data
as ❌.

## 7. Where the code is

| File | What we did |
| --- | --- |
| [bdq.py](bdq.py) | Added the 16 tests and the shared rules (empty, number, date) below the original code |
| [annotations.py](annotations.py) | Added `assess_scope()`, which finds values outside events (location, georeference, identification, taxon identification, specimen root) |
| [main.py](main.py) | Added the `--tests` option and the `BDQ_TESTS` variable; runs all tests by default |
| [test_main.py](test_main.py) | Added 20 unit tests; all 34 tests pass |

## 8. Things the team should agree on

1. **Fixed lists.** Country codes and basisOfRecord values are written in the code,
   not looked up online. If the standards change, the lists must be updated by hand.
2. **Strict matching.** `be` and `preservedspecimen` are rejected. The BDQ spec
   requires rejecting extra spaces; rejecting lowercase is our own choice.
3. **First event only.** dateIdentified is compared with the first event's date.
4. **Whole range must fit.** `dateIdentified = "2026"` fails until 31 Dec 2026,
   because the year has not finished yet.
5. **Week dates** (`2009-W01`) are not supported yet.
6. **Not yet checked against the official TDWG test data.** That is the next step to
   confirm all of the above.
