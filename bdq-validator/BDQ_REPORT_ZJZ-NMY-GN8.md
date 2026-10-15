# Data quality report: SANDBOX/ZJZ-NMY-GN8

Our BDQ Machine Annotation Service checked one DiSSCo specimen against **17 data quality
tests** from the TDWG Biodiversity Data Quality (BDQ) standard.

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

| ✅ Passed | ❌ Failed | ⚪ Could not run |
| :---: | :---: | :---: |
| **12** | **2** | **3** |

- ✅ **Passed**: the data is present and correct.
- ❌ **Failed**: a recommended value is missing.
- ⚪ **Could not run**: the specimen has no data for this test, so there is nothing to check.

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

### 🌿 What is it?

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Any taxon information present | ✅ | 10 taxon terms | The specimen is identified. |
| Scientific name is filled in | ✅ | *Chamaecyparis pisifera* (Siebold & Zucc.) Endl. | |
| Kingdom is filled in | ✅ | `Plantae` | |
| Taxon rank is filled in | ✅ | `SPECIES` | |
| Name author is filled in | ✅ | `(Siebold & Zucc.) Endl.` | |
| **Scientific name ID is filled in** | ❌ | (missing) | No link to a name in a taxonomic database. |
| **Year the name was published is filled in** | ❌ | (missing) | Not recorded. |
| Identification date is plausible | ⚪ | (none) | No identification date recorded. |

### 🏷️ What kind of record is it?

| Test | Result | Value checked | What it means |
| --- | :---: | --- | --- |
| Basis of record is a standard value | ✅ | `PreservedSpecimen` | Uses the official vocabulary. |

## What could be improved

1. **Add a `scientificNameID`.** The record does have a `taxonID` pointing to Catalogue
   of Life (`https://www.catalogueoflife.org/data/taxon/TJJK`). Copying a name
   identifier (for example from IPNI or Catalogue of Life) into `scientificNameID`
   would fix this.
2. **Add `namePublishedInYear`**: the year *Chamaecyparis pisifera* was published.
3. **Optional:** add the elevation and identification date, if they are known. The
   three ⚪ tests would then be able to run.

## How it works

1. The MAS fetches the specimen from DiSSCo in the OpenDS format.
2. It runs each test on the relevant part of the record: the collecting event,
   location, coordinates, identification and taxon.
3. Each result becomes an **annotation** on the specimen. The annotation points at the
   exact field that was checked, so it can be shown next to that field in DiSSCo.
4. No external services are called: all checks run offline and give the same answer
   every time.

The test logic is explained in [BDQ_NEW_TESTS.md](BDQ_NEW_TESTS.md).
