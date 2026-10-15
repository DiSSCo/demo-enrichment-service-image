"""BDQ validations, independent of OpenDS and RabbitMQ."""

import calendar
import math
import re
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any, ClassVar, NamedTuple

try:
    from . import gbif
except ImportError:
    import gbif

TEST_LABEL = "VALIDATION_EVENTDATE_NOTEMPTY"
TEST_VERSION_IRI = "https://rs.tdwg.org/bdqtest/terms/version/f51e15a6-a67d-4729-9c28-3766299d2985-2023-09-17"
IMPLEMENTATION_VERSION = "0.1.0"


class BDQEvent(ABC):
    """Abstract base class for BDQ event validation tests."""

    _registry: ClassVar[list[type["BDQEvent"]]] = []

    test_label: str = ""
    test_version_iri: str = ""
    implementation_name: str = "bdq-test"
    implementation_version: str = IMPLEMENTATION_VERSION
    term: str = "dwc:eventDate"

    TEST_LABEL: str = ""
    TEST_VERSION_IRI: str = ""
    IMPLEMENTATION_NAME: str = "bdq-test"
    IMPLEMENTATION_VERSION: str = IMPLEMENTATION_VERSION
    TERM: str = "dwc:eventDate"

    @classmethod
    def register(cls, test_cls: type["BDQEvent"]) -> type["BDQEvent"]:
        """Register a BDQEvent subclass as a plugin."""
        if test_cls not in cls._registry:
            cls._registry.append(test_cls)
        return test_cls

    @classmethod
    def unregister(cls, test_cls: type["BDQEvent"]) -> None:
        """Unregister a BDQEvent subclass."""
        if test_cls in cls._registry:
            cls._registry.remove(test_cls)

    @classmethod
    def get_registered_tests(cls) -> list[type["BDQEvent"]]:
        """Return all registered test classes."""
        return list(cls._registry)

    @classmethod
    def reset_registry(cls) -> None:
        """Reset registry to the default test(s)."""
        cls._registry = [ValidationEventDateNotEmpty]

    @abstractmethod
    def validate(self, value: str | None) -> dict[str, str]:
        """Assess value and return BDQ response dictionary."""
        raise NotImplementedError


def is_empty(value: str | None) -> bool:
    """BDQ Empty: absent/null, or only characters in U+0000 through U+0020.

    Strings such as "NULL", "unknown", and "?" are NotEmpty. Python's strip()
    also removes some Unicode characters that BDQ treats as actual content.
    """
    return value is None or all(ord(character) <= 0x20 for character in value)


def validate_event_date_notempty(value: str | None) -> dict[str, str]:
    """Assess presence only; date format and interpretation are separate tests."""
    if value is not None and not isinstance(value, str):
        raise TypeError("dwc:eventDate must be a string or null")
    empty = is_empty(value)
    return {
        "Response.status": "RUN_HAS_RESULT",
        "Response.result": "NOT_COMPLIANT" if empty else "COMPLIANT",
        "Response.comment": "dwc:eventDate is empty." if empty else "dwc:eventDate has a value.",
    }


class ValidationEventDateNotEmpty(BDQEvent):
    """Assess whether dwc:eventDate is not empty."""

    test_label = TEST_LABEL
    test_version_iri = TEST_VERSION_IRI
    implementation_name = "bdq-test"
    implementation_version = IMPLEMENTATION_VERSION
    term = "dwc:eventDate"

    TEST_LABEL = test_label
    TEST_VERSION_IRI = test_version_iri
    IMPLEMENTATION_NAME = implementation_name
    IMPLEMENTATION_VERSION = implementation_version
    TERM = term

    def validate(self, value: str | None) -> dict[str, str]:
        return validate_event_date_notempty(value)


BDQEvent._registry = [ValidationEventDateNotEmpty]
BDQEventDateNotEmpty = ValidationEventDateNotEmpty
EventDateNotEmpty = ValidationEventDateNotEmpty


# ---------------------------------------------------------------------------
# Additional TG2 tests. Each one is a BDQEvent plugin like the test above.
#
# Optional attributes read by annotations.test_bdq_event:
# - scope: where the terms live in OpenDS. "event" (default, as above), "location",
#   "georeference", "identification" or "specimen".
# - terms: set when a test reads more than one term. validate() then receives a
#   {term: value} dict instead of a single value.
# - consulted_paths: terms read from elsewhere in the specimen, as key paths from
#   the specimen root, e.g. {"dwc:eventDate": ("ods:hasEvents", 0, "dwc:eventDate")}.
#
# OpenDS stores numbers as JSON numbers (for example dwc:year = 1916), so values
# may be int or float as well as str.
# ---------------------------------------------------------------------------

RUN_HAS_RESULT = "RUN_HAS_RESULT"
INTERNAL_PREREQUISITES_NOT_MET = "INTERNAL_PREREQUISITES_NOT_MET"
EXTERNAL_PREREQUISITES_NOT_MET = "EXTERNAL_PREREQUISITES_NOT_MET"
COMPLIANT = "COMPLIANT"
NOT_COMPLIANT = "NOT_COMPLIANT"
FILLED_IN = "FILLED_IN"
AMENDED = "AMENDED"
NOT_AMENDED = "NOT_AMENDED"

# ISO 3166-1 alpha-2 officially assigned codes (249), plus the user-assigned XZ
# (high seas) and ZZ (unknown), which the test specification says to accept.
_ISO_3166_ALPHA2_CODES = """
    AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO
    BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ
    DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP
    GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG
    KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML
    MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE
    PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL
    SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM
    US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW
    XZ ZZ
"""
ISO_3166_ALPHA2 = frozenset(_ISO_3166_ALPHA2_CODES.split())

# GBIF dwc:basisOfRecord vocabulary, https://rs.gbif.org/vocabulary/dwc/basis_of_record.xml
# (snapshot 2026-10-14).
BASIS_OF_RECORD_VOCABULARY = frozenset(
    {
        "PreservedSpecimen",
        "FossilSpecimen",
        "LivingSpecimen",
        "HumanObservation",
        "MachineObservation",
        "MaterialSample",
        "Occurrence",
    }
)


def bdq_response(status: str, result: str, comment: str) -> dict[str, str]:
    """BDQ response; Response.result is empty when the test could not run."""
    return {"Response.status": status, "Response.result": result, "Response.comment": comment}


def not_run(comment: str) -> dict[str, str]:
    return bdq_response(INTERNAL_PREREQUISITES_NOT_MET, "", comment)


def external_prerequisites_not_met(comment: str) -> dict[str, str]:
    return bdq_response(EXTERNAL_PREREQUISITES_NOT_MET, "", comment)


def compliant(comment: str) -> dict[str, str]:
    return bdq_response(RUN_HAS_RESULT, COMPLIANT, comment)


def not_compliant(comment: str) -> dict[str, str]:
    return bdq_response(RUN_HAS_RESULT, NOT_COMPLIANT, comment)


def filled_in(term: str, value: str, comment: str) -> dict[str, Any]:
    return {
        "Response.status": FILLED_IN,
        "Response.result": {term: value},
        "Response.comment": comment,
    }


def not_amended(comment: str) -> dict[str, str]:
    return bdq_response(NOT_AMENDED, "", comment)


def is_empty_value(value: Any) -> bool:
    """is_empty for OpenDS values: JSON numbers and booleans are never Empty."""
    return value is None or (isinstance(value, str) and is_empty(value))


_NUMBER_RE = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
_INTEGER_RE = re.compile(r"[+-]?\d+")


def as_number(value: Any) -> float | None:
    """Interpret a JSON number or numeric string; None if not a finite number."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str) and _NUMBER_RE.fullmatch(value.strip()):
        return float(value)
    return None


def as_integer(value: Any) -> int | None:
    """Interpret a JSON integer (or whole float) or integer string; None otherwise."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str) and _INTEGER_RE.fullmatch(value.strip()):
        return int(value)
    return None


# ISO 8601 dates ------------------------------------------------------------


class DateRange(NamedTuple):
    """First and last calendar day covered by a date value, inclusive."""

    start: date
    end: date


_TIME_RE = re.compile(r"T(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:[.,]\d+)?)?(?:Z|[+-](?:[01]\d|2[0-3])(?::?[0-5]\d)?)?")
_YEAR_RE = re.compile(r"\d{4}")
_YEAR_MONTH_RE = re.compile(r"(\d{4})-(\d{2})")
_DATE_RE = re.compile(r"(\d{4})-?(\d{2})-?(\d{2})")
_ORDINAL_RE = re.compile(r"(\d{4})-(\d{3})")


def parse_date_range(value: Any) -> DateRange | None:
    """Return the days covered by an ISO 8601 date or interval, or None if invalid.

    "1916" covers the whole year, "1916-07" the whole month, and "2007-11-13/15"
    13 to 15 November 2007. Supported: YYYY, YYYY-MM, YYYY-MM-DD, YYYYMMDD,
    YYYY-DDD, an optional Thh:mm[:ss[.fff]][Z|+hh:mm] time, and start/end
    intervals whose end may omit leading components. Week dates (YYYY-Www) are
    not supported and are treated as invalid.
    """
    if not isinstance(value, str) or not value or value != value.strip():
        return None
    parts = value.split("/")
    if len(parts) == 1:
        return _parse_single_date(value)
    if len(parts) != 2:
        return None
    start = _parse_single_date(parts[0])
    end = _parse_single_date(parts[1]) or _parse_single_date(_expand_interval_end(parts[0], parts[1]))
    if start is None or end is None or end.end < start.start:
        return None
    return DateRange(start.start, end.end)


def _expand_interval_end(start: str, end: str) -> str:
    """Fill in components omitted from an interval end: 2007-11-13/15 -> 2007-11-15."""
    start_parts = start.split("T")[0].split("-")
    end_parts = end.split("-")
    if not 0 < len(end_parts) < len(start_parts) or not all(re.fullmatch(r"\d{2}", p) for p in end_parts):
        return ""
    return "-".join(start_parts[: len(start_parts) - len(end_parts)] + end_parts)


def _parse_single_date(value: str) -> DateRange | None:
    date_part, separator, time_part = value.partition("T")
    if separator and (not _TIME_RE.fullmatch("T" + time_part) or not _DATE_RE.fullmatch(date_part)):
        return None
    try:
        if _YEAR_RE.fullmatch(date_part):
            year = int(date_part)
            return DateRange(date(year, 1, 1), date(year, 12, 31))
        if match := _YEAR_MONTH_RE.fullmatch(date_part):
            year, month = int(match[1]), int(match[2])
            return DateRange(date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1]))
        if match := _DATE_RE.fullmatch(date_part):
            day = date(int(match[1]), int(match[2]), int(match[3]))
            return DateRange(day, day)
        if match := _ORDINAL_RE.fullmatch(date_part):
            year, ordinal = int(match[1]), int(match[2])
            if not 1 <= ordinal <= (366 if calendar.isleap(year) else 365):
                return None
            day = date(year, 1, 1) + timedelta(days=ordinal - 1)
            return DateRange(day, day)
    except ValueError:  # month 13, 29 February in a non-leap year, year 0000, ...
        return None
    return None


# Event tests: same shape as ValidationEventDateNotEmpty ---------------------


class ValidationEventDateStandard(BDQEvent):
    """VALIDATION_EVENTDATE_STANDARD: dwc:eventDate is a valid ISO 8601 date."""

    test_label = "VALIDATION_EVENTDATE_STANDARD"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/4f2bf8fd-fc5c-493f-a44c-e7b16153c803-2024-09-16"
    term = "dwc:eventDate"

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_run("dwc:eventDate is empty.")
        if parse_date_range(value) is None:
            return not_compliant("dwc:eventDate is not a valid ISO 8601 date.")
        return compliant("dwc:eventDate is a valid ISO 8601 date.")


class ValidationEventDateInRange(BDQEvent):
    """VALIDATION_EVENTDATE_INRANGE: dwc:eventDate lies within the valid date range."""

    test_label = "VALIDATION_EVENTDATE_INRANGE"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/3cff4dc4-72e9-4abe-9bf3-8a30f1618432-2024-09-16"
    term = "dwc:eventDate"

    def __init__(
        self, earliest_valid_date: str = "1582-11-15", latest_valid_date: str | None = None, today: date | None = None
    ):
        # bdq:latestValidDate defaults to {current year}, i.e. through 31 December.
        today = today or date.today()
        self.earliest = parse_date_range(earliest_valid_date).start
        self.latest = parse_date_range(latest_valid_date or str(today.year)).end

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_run("dwc:eventDate is empty.")
        dates = parse_date_range(value)
        if dates is None:
            return not_run("dwc:eventDate is not a valid ISO 8601 date.")
        if self.earliest <= dates.start and dates.end <= self.latest:
            return compliant(f"dwc:eventDate is within {self.earliest} to {self.latest}.")
        return not_compliant(f"dwc:eventDate is not entirely within {self.earliest} to {self.latest}.")


class ValidationYearInRange(BDQEvent):
    """VALIDATION_YEAR_INRANGE: dwc:year lies within the valid year range."""

    test_label = "VALIDATION_YEAR_INRANGE"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/ad0c8855-de69-4843-a80c-a5387d20fbc8-2024-08-23"
    term = "dwc:year"

    def __init__(
        self, earliest_valid_year: int = 1582, latest_valid_year: int | None = None, today: date | None = None
    ):
        # bdq:latestValidDate defaults to {current year}.
        self.earliest = earliest_valid_year
        self.latest = latest_valid_year if latest_valid_year is not None else (today or date.today()).year

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_run("dwc:year is empty.")
        year = as_integer(value)
        if year is None:
            return not_run("dwc:year cannot be interpreted as an integer.")
        if self.earliest <= year <= self.latest:
            return compliant(f"dwc:year is within {self.earliest} to {self.latest}.")
        return not_compliant(f"dwc:year is outside {self.earliest} to {self.latest}.")


# Location tests: terms in $['ods:hasEvents'][i]['ods:hasLocation'] -----------


class _ElevationInRange(BDQEvent):
    scope = "location"

    def __init__(self, minimum_valid_elevation: float = -430, maximum_valid_elevation: float = 8850):
        self.minimum = minimum_valid_elevation
        self.maximum = maximum_valid_elevation

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_run(f"{self.term} is empty.")
        elevation = as_number(value)
        if elevation is None:
            return not_run(f"{self.term} is not a number.")
        if self.minimum <= elevation <= self.maximum:
            return compliant(f"{self.term} is within {self.minimum} to {self.maximum} m.")
        return not_compliant(f"{self.term} is outside {self.minimum} to {self.maximum} m.")


class ValidationMinElevationInRange(_ElevationInRange):
    """VALIDATION_MINELEVATION_INRANGE: dwc:minimumElevationInMeters is plausible."""

    test_label = "VALIDATION_MINELEVATION_INRANGE"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/0bb8297d-8f8a-42d2-80c1-558f29efe798-2023-09-17"
    term = "dwc:minimumElevationInMeters"


class ValidationMaxElevationInRange(_ElevationInRange):
    """VALIDATION_MAXELEVATION_INRANGE: dwc:maximumElevationInMeters is plausible."""

    test_label = "VALIDATION_MAXELEVATION_INRANGE"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/c971fe3f-84c1-4636-9f44-b1ec31fd63c7-2023-09-18"
    term = "dwc:maximumElevationInMeters"


class ValidationCountryCodeStandard(BDQEvent):
    """VALIDATION_COUNTRYCODE_STANDARD: dwc:countryCode is an ISO 3166-1 alpha-2 code."""

    test_label = "VALIDATION_COUNTRYCODE_STANDARD"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/0493bcfb-652e-4d17-815b-b0cce0742fbe-2024-09-19"
    term = "dwc:countryCode"
    scope = "location"

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_run("dwc:countryCode is empty.")
        # Exact match: surrounding whitespace or lowercase is NOT_COMPLIANT.
        if value in ISO_3166_ALPHA2:
            return compliant("dwc:countryCode is a valid ISO 3166-1 alpha-2 code.")
        return not_compliant("dwc:countryCode is not a valid ISO 3166-1 alpha-2 code.")


# Georeference test: terms in $['ods:hasEvents'][i]['ods:hasLocation']['ods:hasGeoreference']


class ValidationCoordinatesNotZero(BDQEvent):
    """VALIDATION_COORDINATES_NOTZERO: latitude and longitude are not both zero."""

    test_label = "VALIDATION_COORDINATES_NOTZERO"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/1bf0e210-6792-4128-b8cc-ab6828aa4871-2023-06-20"
    term = "dwc:decimalLatitude"
    terms = ("dwc:decimalLatitude", "dwc:decimalLongitude")
    scope = "georeference"

    def validate(self, value: dict[str, Any]) -> dict[str, str]:
        numbers = []
        for term in self.terms:
            if is_empty_value(value.get(term)):
                return not_run(f"{term} is empty.")
            number = as_number(value[term])
            if number is None:
                return not_run(f"{term} is not a number.")
            numbers.append(number)
        if any(number != 0 for number in numbers):
            return compliant("The coordinates are not 0, 0.")
        return not_compliant("dwc:decimalLatitude and dwc:decimalLongitude are both 0.")


# Identification test: terms in $['ods:hasIdentifications'][i] --------------


class ValidationDateIdentifiedInRange(BDQEvent):
    """VALIDATION_DATEIDENTIFIED_INRANGE: dwc:dateIdentified is plausible and not before the event.

    dwc:eventDate is consulted from the first event, the collecting event in OpenDS.
    """

    test_label = "VALIDATION_DATEIDENTIFIED_INRANGE"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/dc8aae4b-134f-4d75-8a71-c4186239178e-2024-09-16"
    term = "dwc:dateIdentified"
    terms = ("dwc:dateIdentified", "dwc:eventDate")
    scope = "identification"
    consulted_paths: ClassVar[dict[str, tuple]] = {"dwc:eventDate": ("ods:hasEvents", 0, "dwc:eventDate")}

    def __init__(
        self,
        earliest_valid_date: str = "1753-01-01",
        latest_valid_date: str | None = None,
        include_event_date: bool = True,
        today: date | None = None,
    ):
        # bdq:latestValidDate defaults to {current day}.
        self.earliest = parse_date_range(earliest_valid_date).start
        self.latest = parse_date_range(latest_valid_date).end if latest_valid_date else (today or date.today())
        self.include_event_date = include_event_date

    def validate(self, value: dict[str, Any]) -> dict[str, str]:
        identified, event_date = value.get("dwc:dateIdentified"), value.get("dwc:eventDate")
        if is_empty_value(identified):
            return not_run("dwc:dateIdentified is empty.")
        identified_dates = parse_date_range(identified)
        if identified_dates is None:
            return not_run("dwc:dateIdentified is not a valid ISO 8601 date.")
        event_dates = None
        if self.include_event_date and not is_empty_value(event_date):
            event_dates = parse_date_range(event_date)
            if event_dates is None:
                return not_run("dwc:eventDate is not a valid ISO 8601 date.")
        if not (self.earliest <= identified_dates.start and identified_dates.end <= self.latest):
            return not_compliant(f"dwc:dateIdentified is outside {self.earliest} to {self.latest}.")
        if event_dates is None:
            return compliant(f"dwc:dateIdentified is within {self.earliest} to {self.latest}.")
        # "Overlaps or is later than" dwc:eventDate.
        if identified_dates.end < event_dates.start:
            return not_compliant("dwc:dateIdentified is before dwc:eventDate.")
        return compliant(f"dwc:dateIdentified is within {self.earliest} to {self.latest} and not before dwc:eventDate.")


# Specimen test: terms at the root of the Digital Specimen -------------------


class ValidationBasisOfRecordStandard(BDQEvent):
    """VALIDATION_BASISOFRECORD_STANDARD: dwc:basisOfRecord is in the GBIF vocabulary."""

    test_label = "VALIDATION_BASISOFRECORD_STANDARD"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/42408a00-bf71-4892-a399-4325e2bc1fb8-2024-08-18"
    term = "dwc:basisOfRecord"
    scope = "specimen"

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_run("dwc:basisOfRecord is empty.")
        # Exact match: surrounding whitespace or other capitalisation is NOT_COMPLIANT.
        if value in BASIS_OF_RECORD_VOCABULARY:
            return compliant("dwc:basisOfRecord is in the GBIF basisOfRecord vocabulary.")
        return not_compliant("dwc:basisOfRecord is not in the GBIF basisOfRecord vocabulary.")


# Taxon tests: terms in $['ods:hasIdentifications'][i]['ods:hasTaxonIdentifications'][j]
#
# NOTEMPTY tests always run: an empty value is NOT_COMPLIANT, not a prerequisite failure.


class _TaxonTermNotEmpty(BDQEvent):
    scope = "taxon"

    def validate(self, value: Any) -> dict[str, str]:
        if is_empty_value(value):
            return not_compliant(f"{self.term} is empty.")
        return compliant(f"{self.term} has a value.")


class ValidationScientificNameNotEmpty(_TaxonTermNotEmpty):
    """VALIDATION_SCIENTIFICNAME_NOTEMPTY: dwc:scientificName has a value."""

    test_label = "VALIDATION_SCIENTIFICNAME_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/7c4b9498-a8d9-4ebb-85f1-9f200c788595-2023-09-18"
    term = "dwc:scientificName"


class ValidationKingdomNotEmpty(_TaxonTermNotEmpty):
    """VALIDATION_KINGDOM_NOTEMPTY: dwc:kingdom has a value."""

    test_label = "VALIDATION_KINGDOM_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/36ed36c9-b1a7-40b2-b5e2-0d012e772098-2024-01-28"
    term = "dwc:kingdom"


class ValidationTaxonRankNotEmpty(_TaxonTermNotEmpty):
    """VALIDATION_TAXONRANK_NOTEMPTY: dwc:taxonRank has a value."""

    test_label = "VALIDATION_TAXONRANK_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/14da5b87-8304-4b2b-911d-117e3c29e890-2023-09-18"
    term = "dwc:taxonRank"


class ValidationScientificNameIdNotEmpty(_TaxonTermNotEmpty):
    """VALIDATION_SCIENTIFICNAMEID_NOTEMPTY: dwc:scientificNameID has a value."""

    test_label = "VALIDATION_SCIENTIFICNAMEID_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/401bf207-9a55-4dff-88a5-abcd58ad97fa-2023-09-18"
    term = "dwc:scientificNameID"


class ValidationScientificNameAuthorshipNotEmpty(_TaxonTermNotEmpty):
    """VALIDATION_SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY: dwc:scientificNameAuthorship has a value."""

    test_label = "VALIDATION_SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/49f1d386-5bed-43ae-bd43-deabf7df64fc-2024-02-04"
    term = "dwc:scientificNameAuthorship"


class ValidationNamePublishedInYearNotEmpty(_TaxonTermNotEmpty):
    """VALIDATION_NAMEPUBLISHEDINYEAR_NOTEMPTY: dwc:namePublishedInYear has a value."""

    test_label = "VALIDATION_NAMEPUBLISHEDINYEAR_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/ff59f77d-71e9-4eb1-aac9-8bd05c50ff70-2024-02-07"
    term = "dwc:namePublishedInYear"


class ValidationTaxonNotEmpty(BDQEvent):
    """VALIDATION_TAXON_NOTEMPTY: at least one term naming the taxon has a value.

    parentNameUsageID, originalNameUsageID, taxonConceptID and higherClassification
    are not part of ods:TaxonIdentification, so they are always absent there.
    """

    test_label = "VALIDATION_TAXON_NOTEMPTY"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/06851339-843f-4a43-8422-4e61b9a00e75-2023-09-18"
    term = "dwc:scientificName"
    terms = (
        "dwc:taxonID",
        "dwc:scientificNameID",
        "dwc:acceptedNameUsageID",
        "dwc:parentNameUsageID",
        "dwc:originalNameUsageID",
        "dwc:taxonConceptID",
        "dwc:scientificName",
        "dwc:higherClassification",
        "dwc:kingdom",
        "dwc:phylum",
        "dwc:class",
        "dwc:order",
        "dwc:superfamily",
        "dwc:family",
        "dwc:subfamily",
        "dwc:tribe",
        "dwc:subtribe",
        "dwc:genus",
        "dwc:genericName",
        "dwc:subgenus",
        "dwc:infragenericEpithet",
        "dwc:specificEpithet",
        "dwc:infraspecificEpithet",
        "dwc:vernacularName",
        "dwc:cultivarEpithet",
    )
    scope = "taxon"

    def validate(self, value: dict[str, Any]) -> dict[str, str]:
        filled = [term for term in self.terms if not is_empty_value(value.get(term))]
        if filled:
            return compliant(f"Taxon terms with a value: {', '.join(filled)}.")
        return not_compliant("No term identifying the taxon has a value.")


# Every implemented test, in run order. main.py runs all of them unless --tests
# selects some. The registry above (used when test_bdq_event gets no tests) still
# holds only ValidationEventDateNotEmpty.


# GBIF Backbone Taxonomy tests (Tier 5) -------------------------------------


class _TaxonRankFound(BDQEvent):
    scope = "taxon"
    rank: str = ""

    def validate(self, value: Any) -> dict[str, Any]:
        if is_empty_value(value):
            return not_run(f"{self.term} is empty.")
        val_str = str(value).strip()
        try:
            results = gbif.query_species(val_str)
        except gbif.GBIFServiceError:
            return external_prerequisites_not_met("The GBIF Backbone Taxonomy service is unavailable or unreachable.")
        target_rank = self.rank.upper()
        for item in results:
            if item.get("rank") == target_rank:
                return compliant(
                    f"{self.term} occurs at the rank of {self.rank.capitalize()} in the GBIF Backbone Taxonomy."
                )
        return not_compliant(
            f"{self.term} does not occur at the rank of {self.rank.capitalize()} in the GBIF Backbone Taxonomy."
        )


class ValidationKingdomFound(_TaxonRankFound):
    """VALIDATION_KINGDOM_FOUND: dwc:kingdom occurs at rank of Kingdom in GBIF Backbone."""

    test_label = "VALIDATION_KINGDOM_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/125b5493-052d-4a0d-a3e1-ed5bf792689e-2023-09-18"
    term = "dwc:kingdom"
    rank = "KINGDOM"


class ValidationPhylumFound(_TaxonRankFound):
    """VALIDATION_PHYLUM_FOUND: dwc:phylum occurs at rank of Phylum in GBIF Backbone."""

    test_label = "VALIDATION_PHYLUM_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/eaad41c5-1d46-4917-a08b-4fd1d7ff5c0f-2022-03-25"
    term = "dwc:phylum"
    rank = "PHYLUM"


class ValidationClassFound(_TaxonRankFound):
    """VALIDATION_CLASS_FOUND: dwc:class occurs at rank of Class in GBIF Backbone."""

    test_label = "VALIDATION_CLASS_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/2cd6884e-3d14-4476-94f7-1191cfff309b-2023-09-18"
    term = "dwc:class"
    rank = "CLASS"


class ValidationOrderFound(_TaxonRankFound):
    """VALIDATION_ORDER_FOUND: dwc:order occurs at rank of Order in GBIF Backbone."""

    test_label = "VALIDATION_ORDER_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/81cc974d-43cc-4c0f-a5e0-afa23b455aa3-2023-09-18"
    term = "dwc:order"
    rank = "ORDER"


class ValidationFamilyFound(_TaxonRankFound):
    """VALIDATION_FAMILY_FOUND: dwc:family occurs at rank of Family in GBIF Backbone."""

    test_label = "VALIDATION_FAMILY_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/3667556d-d8f5-454c-922b-af8af38f613c-2023-09-17"
    term = "dwc:family"
    rank = "FAMILY"


class ValidationGenusFound(_TaxonRankFound):
    """VALIDATION_GENUS_FOUND: dwc:genus occurs at rank of Genus in GBIF Backbone."""

    test_label = "VALIDATION_GENUS_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/f2ce7d55-5b1d-426a-b00e-6d4efe3058ec-2023-09-18"
    term = "dwc:genus"
    rank = "GENUS"


class ValidationScientificNameFound(BDQEvent):
    """VALIDATION_SCIENTIFICNAME_FOUND: dwc:scientificName matches a record in GBIF Backbone."""

    test_label = "VALIDATION_SCIENTIFICNAME_FOUND"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/3f335517-f442-4b98-b149-1e87ff16de45-2023-09-17"
    term = "dwc:scientificName"
    scope = "taxon"

    def validate(self, value: Any) -> dict[str, Any]:
        if is_empty_value(value):
            return not_run("dwc:scientificName is empty.")
        val_str = str(value).strip()
        try:
            results = gbif.query_species(val_str)
            matched = bool(results)
            if not matched:
                match = gbif.match_species(val_str, strict=True)
                matched = match.get("matchType") == "EXACT" and bool(match.get("usageKey"))
        except gbif.GBIFServiceError:
            return external_prerequisites_not_met("The GBIF Backbone Taxonomy service is unavailable or unreachable.")
        if matched:
            return compliant(f"dwc:scientificName '{val_str}' was found in the GBIF Backbone Taxonomy.")
        return not_compliant(f"dwc:scientificName '{val_str}' was not found in the GBIF Backbone Taxonomy.")


class ValidationClassificationConsistent(BDQEvent):
    """VALIDATION_CLASSIFICATION_CONSISTENT: higher classification terms are consistent in GBIF Backbone."""

    test_label = "VALIDATION_CLASSIFICATION_CONSISTENT"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/2750c040-1d4a-4149-99fe-0512785f2d5f-2023-09-18"
    term = "dwc:scientificName"
    terms = (
        "dwc:kingdom",
        "dwc:phylum",
        "dwc:class",
        "dwc:order",
        "dwc:superfamily",
        "dwc:family",
        "dwc:subfamily",
        "dwc:tribe",
        "dwc:subtribe",
        "dwc:genus",
    )
    scope = "taxon"

    def validate(self, value: dict[str, Any]) -> dict[str, Any]:
        filled_terms = {t: str(value[t]).strip() for t in self.terms if not is_empty_value(value.get(t))}
        if not filled_terms:
            return not_run("All higher classification terms are empty.")

        rank_order = [
            "dwc:genus",
            "dwc:subtribe",
            "dwc:tribe",
            "dwc:subfamily",
            "dwc:family",
            "dwc:superfamily",
            "dwc:order",
            "dwc:class",
            "dwc:phylum",
            "dwc:kingdom",
        ]
        lowest_term = None
        for t in rank_order:
            if t in filled_terms:
                lowest_term = t
                break

        lowest_val = filled_terms[lowest_term]
        rank_name = lowest_term.removeprefix("dwc:").upper()
        try:
            match = gbif.match_species(name=lowest_val, rank=rank_name, strict=True)
            if match.get("matchType") == "NONE" or not match.get("usageKey"):
                candidates = gbif.query_species(lowest_val)
                matched_candidates = [c for c in candidates if c.get("rank") == rank_name]
                if not matched_candidates:
                    return not_compliant(
                        f"Lowest classification term {lowest_term}='{lowest_val}' could not be matched in the GBIF Backbone Taxonomy."
                    )
                match = matched_candidates[0]
        except gbif.GBIFServiceError:
            return external_prerequisites_not_met("The GBIF Backbone Taxonomy service is unavailable or unreachable.")

        for t, term_val in filled_terms.items():
            if t == lowest_term:
                continue
            gbif_field = t.removeprefix("dwc:").lower()
            expected = match.get(gbif_field)
            if expected and str(expected).strip().lower() != term_val.lower():
                return not_compliant(
                    f"Higher classification term {t}='{term_val}' is inconsistent with '{expected}' in the GBIF Backbone Taxonomy."
                )
        return compliant("Higher classification terms are consistent in the GBIF Backbone Taxonomy.")


class ValidationTaxonUnambiguous(BDQEvent):
    """VALIDATION_TAXON_UNAMBIGUOUS: taxon can be unambiguously resolved in GBIF Backbone."""

    test_label = "VALIDATION_TAXON_UNAMBIGUOUS"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/4c09f127-737b-4686-82a0-7c8e30841590-2023-09-18"
    term = "dwc:scientificName"
    terms = (
        "dwc:taxonID",
        "dwc:scientificNameID",
        "dwc:acceptedNameUsageID",
        "dwc:originalNameUsageID",
        "dwc:taxonConceptID",
        "dwc:scientificName",
        "dwc:higherClassification",
        "dwc:kingdom",
        "dwc:phylum",
        "dwc:class",
        "dwc:order",
        "dwc:superfamily",
        "dwc:family",
        "dwc:subfamily",
        "dwc:tribe",
        "dwc:subtribe",
        "dwc:genus",
        "dwc:genericName",
        "dwc:subgenus",
        "dwc:infragenericEpithet",
        "dwc:specificEpithet",
        "dwc:infraspecificEpithet",
        "dwc:cultivarEpithet",
        "dwc:vernacularName",
        "dwc:scientificNameAuthorship",
        "dwc:taxonRank",
    )
    scope = "taxon"

    def validate(self, value: dict[str, Any]) -> dict[str, Any]:
        sci_id = value.get("dwc:scientificNameID")
        has_sci_id = not is_empty_value(sci_id)
        sci_name = value.get("dwc:scientificName")
        authorship = value.get("dwc:scientificNameAuthorship")
        genus = value.get("dwc:genus") or value.get("dwc:genericName")
        spec_ep = value.get("dwc:specificEpithet")
        infra_ep = value.get("dwc:infraspecificEpithet")
        cultivar_ep = value.get("dwc:cultivarEpithet")

        if (
            not has_sci_id
            and is_empty_value(sci_name)
            and is_empty_value(genus)
            and is_empty_value(spec_ep)
            and is_empty_value(infra_ep)
            and is_empty_value(authorship)
            and is_empty_value(cultivar_ep)
        ):
            return not_run("All relevant taxon naming values are empty.")

        try:
            if has_sci_id:
                key = gbif.parse_gbif_key(sci_id)
                if key is None:
                    return not_compliant(f"dwc:scientificNameID '{sci_id}' contains an uninterpretable value.")
                rec = gbif.get_species_by_key(key)
                if rec and rec.get("key"):
                    return compliant(
                        f"dwc:scientificNameID '{sci_id}' unambiguously references a single taxon record in the GBIF Backbone."
                    )
                return not_compliant(f"dwc:scientificNameID '{sci_id}' could not be resolved in the GBIF Backbone.")

            name_query = str(sci_name).strip() if not is_empty_value(sci_name) else ""
            if not name_query:
                parts = [p for p in (genus, spec_ep, infra_ep, cultivar_ep) if not is_empty_value(p)]
                name_query = " ".join(str(p).strip() for p in parts)

            if authorship and not is_empty_value(authorship) and str(authorship).strip() not in name_query:
                name_query = f"{name_query} {str(authorship).strip()}".strip()

            match = gbif.match_species(
                name=name_query,
                kingdom=value.get("dwc:kingdom"),
                phylum=value.get("dwc:phylum"),
                class_=value.get("dwc:class"),
                order=value.get("dwc:order"),
                family=value.get("dwc:family"),
                genus=value.get("dwc:genus") or value.get("dwc:genericName"),
                rank=value.get("dwc:taxonRank"),
            )
            if (
                match.get("matchType") in ("EXACT", "FUZZY")
                and match.get("usageKey")
                and match.get("confidence", 0) >= 80
            ):
                return compliant(
                    f"Taxon unambiguously resolved in the GBIF Backbone to '{match.get('scientificName')}' (usageKey {match.get('usageKey')})."
                )
            return not_compliant("Taxon could not be unambiguously resolved in the GBIF Backbone Taxonomy.")
        except gbif.GBIFServiceError:
            return external_prerequisites_not_met("The GBIF Backbone Taxonomy service is unavailable or unreachable.")


class AmendmentScientificNameFromScientificNameId(BDQEvent):
    """AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID: fills in dwc:scientificName from dwc:scientificNameID."""

    test_label = "AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/f01fb3f9-2f7e-418b-9f51-adf50f202aea-2024-08-18"
    term = "dwc:scientificName"
    terms = ("dwc:scientificName", "dwc:scientificNameID")
    scope = "taxon"

    def validate(self, value: dict[str, Any]) -> dict[str, Any]:
        sci_id = value.get("dwc:scientificNameID")
        sci_name = value.get("dwc:scientificName")
        if is_empty_value(sci_id) or not is_empty_value(sci_name):
            return not_run("dwc:scientificNameID is empty or dwc:scientificName is not empty.")

        key = gbif.parse_gbif_key(sci_id)
        if key is None:
            return not_amended("dwc:scientificNameID does not contain an interpretable value.")
        try:
            rec = gbif.get_species_by_key(key)
            if rec and rec.get("scientificName"):
                return filled_in(
                    "dwc:scientificName",
                    rec["scientificName"],
                    f"dwc:scientificName filled in from GBIF Backbone for usageKey {key}.",
                )
            return not_amended(f"No taxon found in the GBIF Backbone for scientificNameID '{sci_id}'.")
        except gbif.GBIFServiceError:
            return external_prerequisites_not_met("The GBIF Backbone Taxonomy service is unavailable or unreachable.")


class AmendmentScientificNameIdFromTaxon(BDQEvent):
    """AMENDMENT_SCIENTIFICNAMEID_FROM_TAXON: proposes dwc:scientificNameID from taxon terms."""

    test_label = "AMENDMENT_SCIENTIFICNAMEID_FROM_TAXON"
    test_version_iri = "https://rs.tdwg.org/bdqtest/terms/version/431467d6-9b4b-48fa-a197-cd5379f5e889-2023-09-17"
    term = "dwc:scientificNameID"
    terms = (
        "dwc:taxonID",
        "dwc:scientificNameID",
        "dwc:acceptedNameUsageID",
        "dwc:originalNameUsageID",
        "dwc:taxonConceptID",
        "dwc:scientificName",
        "dwc:higherClassification",
        "dwc:kingdom",
        "dwc:phylum",
        "dwc:class",
        "dwc:order",
        "dwc:superfamily",
        "dwc:family",
        "dwc:subfamily",
        "dwc:tribe",
        "dwc:subtribe",
        "dwc:genus",
        "dwc:genericName",
        "dwc:subgenus",
        "dwc:specificEpithet",
        "dwc:infraspecificEpithet",
        "dwc:cultivarEpithet",
        "dwc:vernacularName",
        "dwc:scientificNameAuthorship",
        "dwc:taxonRank",
    )
    scope = "taxon"

    def validate(self, value: dict[str, Any]) -> dict[str, Any]:
        sci_id = value.get("dwc:scientificNameID")
        if not is_empty_value(sci_id):
            return not_run("dwc:scientificNameID is not empty.")

        sci_name = value.get("dwc:scientificName")
        authorship = value.get("dwc:scientificNameAuthorship")
        genus = value.get("dwc:genus") or value.get("dwc:genericName")
        spec_ep = value.get("dwc:specificEpithet")
        infra_ep = value.get("dwc:infraspecificEpithet")
        cultivar_ep = value.get("dwc:cultivarEpithet")

        if (
            is_empty_value(sci_name)
            and is_empty_value(genus)
            and is_empty_value(spec_ep)
            and is_empty_value(infra_ep)
            and is_empty_value(authorship)
            and is_empty_value(cultivar_ep)
        ):
            return not_run("All relevant taxon naming values are empty.")

        name_query = str(sci_name).strip() if not is_empty_value(sci_name) else ""
        if not name_query:
            parts = [p for p in (genus, spec_ep, infra_ep, cultivar_ep) if not is_empty_value(p)]
            name_query = " ".join(str(p).strip() for p in parts)

        if authorship and not is_empty_value(authorship) and str(authorship).strip() not in name_query:
            name_query = f"{name_query} {str(authorship).strip()}".strip()

        try:
            match = gbif.match_species(
                name=name_query,
                kingdom=value.get("dwc:kingdom"),
                phylum=value.get("dwc:phylum"),
                class_=value.get("dwc:class"),
                order=value.get("dwc:order"),
                family=value.get("dwc:family"),
                genus=value.get("dwc:genus") or value.get("dwc:genericName"),
                rank=value.get("dwc:taxonRank"),
            )
            if (
                match.get("matchType") in ("EXACT", "FUZZY")
                and match.get("usageKey")
                and match.get("confidence", 0) >= 80
            ):
                usage_key = match["usageKey"]
                proposed_id = f"gbif:{usage_key}"
                return filled_in(
                    "dwc:scientificNameID",
                    proposed_id,
                    f"dwc:scientificNameID filled in from GBIF Backbone for '{match.get('scientificName')}'.",
                )
            return not_amended("Taxon is ambiguous or could not be resolved in the GBIF Backbone Taxonomy.")
        except gbif.GBIFServiceError:
            return external_prerequisites_not_met("The GBIF Backbone Taxonomy service is unavailable or unreachable.")


ALL_TESTS: list[type[BDQEvent]] = [
    ValidationEventDateNotEmpty,
    ValidationEventDateStandard,
    ValidationEventDateInRange,
    ValidationYearInRange,
    ValidationMinElevationInRange,
    ValidationMaxElevationInRange,
    ValidationCountryCodeStandard,
    ValidationCoordinatesNotZero,
    ValidationDateIdentifiedInRange,
    ValidationBasisOfRecordStandard,
    ValidationScientificNameNotEmpty,
    ValidationKingdomNotEmpty,
    ValidationTaxonRankNotEmpty,
    ValidationScientificNameIdNotEmpty,
    ValidationScientificNameAuthorshipNotEmpty,
    ValidationNamePublishedInYearNotEmpty,
    ValidationTaxonNotEmpty,
    ValidationKingdomFound,
    ValidationPhylumFound,
    ValidationClassFound,
    ValidationOrderFound,
    ValidationFamilyFound,
    ValidationGenusFound,
    ValidationScientificNameFound,
    ValidationClassificationConsistent,
    ValidationTaxonUnambiguous,
    AmendmentScientificNameFromScientificNameId,
    AmendmentScientificNameIdFromTaxon,
]
TESTS_BY_LABEL: dict[str, type[BDQEvent]] = {test.test_label: test for test in ALL_TESTS}
