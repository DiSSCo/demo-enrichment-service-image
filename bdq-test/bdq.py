"""BDQ validations, independent of OpenDS and RabbitMQ."""

from abc import ABC, abstractmethod
from typing import ClassVar

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
