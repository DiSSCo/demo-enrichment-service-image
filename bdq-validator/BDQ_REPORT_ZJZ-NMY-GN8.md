# Data quality report: SANDBOX/ZJZ-NMY-GN8

Our BDQ Machine Annotation Service checked one DiSSCo specimen against **28 data quality
tests** from the TDWG Biodiversity Data Quality (BDQ) standard, including local format/range
checks and authoritative GBIF Backbone Taxonomy validations and amendments.

> Generated with `python3 bdq-validator/main.py --specimen SANDBOX/ZJZ-NMY-GN8`
> on 2026-10-14, against specimen version 4.

## The specimen

| | |
| --- | --- |
| **Name** | *Chamaecyparis pisifera* (Siebold & Zucc.) Endl. (Sawara cypress) |
| **Institution** | Meise Botanic Garden |
| **Type** | Preserved specimen (botany) |
| **Collected** | July 1916, Mechelen, Belgium (garden of Prof. Bellis) |
| **DiSSCo page** | [sandbox.dissco.tech/ds/SANDBOX/ZJZ-NMY-GN8](https://sandbox.dissco.tech/ds/SANDBOX/ZJZ-NMY-GN8) |

## Result at a glance

| ✅ Passed | ❌ Failed | ⚪ Could not run | 💡 Proposed amendment |
| :---: | :---: | :---: | :---: |
| **22** | **2** | **3** | **1** |

- ✅ **Passed**: the data is present and correct (COMPLIANT).
- ❌ **Failed**: a recommended value is missing or invalid (NOT_COMPLIANT).
- ⚪ **Could not run**: the specimen has no data for this test, so prerequisites are not met.
- 💡 **Proposed amendment**: the service proposed an authoritative value from the GBIF Backbone Taxonomy.

## Results by topic

### 📅 When was it collected?

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Event date is filled in | ✅ | `1916-07` | A collecting date is recorded. |
| Event date is a valid date | ✅ | `1916-07` | Correct ISO 8601 format (July 1916). |
| Event date is plausible | ✅ | `1916-07` | Between 1582 and today. |
| Year is plausible | ✅ | `1916` | Between 1582 and today. |

### 🌍 Where was it collected?

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Country code is valid | ✅ | `BE` | A real ISO country code (Belgium). |
| Coordinates are not 0, 0 | ✅ | `51.026111, 4.478055` | Real coordinates, not a placeholder. |
| Minimum elevation is plausible | ⚪ | (none) | No elevation recorded. |
| Maximum elevation is plausible | ⚪ | (none) | No elevation recorded. |

### 🌿 What is it? (Local checks)

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Any taxon information present | ✅ | 10 taxon terms | The specimen is identified. |
| Scientific name is filled in | ✅ | *Chamaecyparis pisifera* (Siebold & Zucc.) Endl. | Taxonomic name present. |
| Kingdom is filled in | ✅ | `Plantae` | Kingdom recorded. |
| Taxon rank is filled in | ✅ | `SPECIES` | Rank recorded. |
| Name author is filled in | ✅ | `(Siebold & Zucc.) Endl.` | Authorship recorded. |
| **Scientific name ID is filled in** | ❌ | (missing) | No link to a name in a taxonomic database. |
| **Year the name was published is filled in** | ❌ | (missing) | Publication year not recorded. |
| Identification date is plausible | ⚪ | (none) | No identification date recorded. |

### 🧬 What is it? (GBIF Backbone Taxonomy checks)

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Kingdom found in GBIF Backbone | ✅ | `Plantae` | Validated at rank Kingdom in GBIF. |
| Phylum found in GBIF Backbone | ✅ | `Tracheophyta` | Validated at rank Phylum in GBIF. |
| Class found in GBIF Backbone | ✅ | `Pinopsida` | Validated at rank Class in GBIF. |
| Order found in GBIF Backbone | ✅ | `Pinales` | Validated at rank Order in GBIF. |
| Family found in GBIF Backbone | ✅ | `Cupressaceae` | Validated at rank Family in GBIF. |
| Genus found in GBIF Backbone | ✅ | `Chamaecyparis` | Validated at rank Genus in GBIF. |
| Scientific name found in GBIF Backbone | ✅ | *Chamaecyparis pisifera* (Siebold & Zucc.) Endl. | Exact match in GBIF Backbone nub. |
| Higher classification consistent | ✅ | 10 classification terms | Hierarchy matches GBIF Backbone taxonomy. |
| Taxon is unambiguous | ✅ | *Chamaecyparis pisifera* | Unambiguously resolved to GBIF usageKey `2683880`. |
| Scientific name from scientificNameID | ⚪ | (missing ID) | Not applicable because name is already present and ID is missing. |
| **Scientific name ID proposed from taxon** | 💡 | `gbif:2683880` | Proposed authoritative `dwc:scientificNameID`. |

### 🏷️ What kind of record is it?

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Basis of record is a standard value | ✅ | `PreservedSpecimen` | Uses the official vocabulary. |

## What could be improved

1. **Accept the proposed `scientificNameID`.** The service proposed `gbif:2683880`, linking
   the specimen directly to the GBIF Backbone record.
2. **Add `namePublishedInYear`**: the year *Chamaecyparis pisifera* was published (`1847`).
3. **Optional:** add the elevation and identification date if known, enabling the ⚪ tests
   to run.

## How it works

1. The MAS fetches the specimen from DiSSCo in the OpenDS format.
2. It runs each test on the relevant part of the record: the collecting event,
   location, coordinates, identification and taxon.
3. Each result becomes an **annotation** on the specimen. The annotation points at the
   exact field that was checked, so it can be shown next to that field in DiSSCo.
4. No external services are called: all checks run offline and give the same answer
   every time.

The test logic is explained in [BDQ_NEW_TESTS.md](BDQ_NEW_TESTS.md).
