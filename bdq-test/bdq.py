"""One BDQ validation, independent of OpenDS and RabbitMQ."""

TEST_LABEL = "VALIDATION_EVENTDATE_NOTEMPTY"
TEST_VERSION_IRI = "https://rs.tdwg.org/bdqtest/terms/version/f51e15a6-a67d-4729-9c28-3766299d2985-2023-09-17"
IMPLEMENTATION_VERSION = "0.1.0"


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
