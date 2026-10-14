"""Offline checks for BDQ semantics, specimen mapping, and message handling."""

import copy
import json
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main
from annotations import build_mas_annotation, test_bdq_event
from bdq import (
    ALL_TESTS,
    ISO_3166_ALPHA2,
    BDQEvent,
    ValidationBasisOfRecordStandard,
    ValidationCoordinatesNotZero,
    ValidationCountryCodeStandard,
    ValidationDateIdentifiedInRange,
    ValidationEventDateInRange,
    ValidationEventDateNotEmpty,
    ValidationEventDateStandard,
    ValidationKingdomNotEmpty,
    ValidationMaxElevationInRange,
    ValidationMinElevationInRange,
    ValidationNamePublishedInYearNotEmpty,
    ValidationScientificNameAuthorshipNotEmpty,
    ValidationScientificNameIdNotEmpty,
    ValidationScientificNameNotEmpty,
    ValidationTaxonNotEmpty,
    ValidationTaxonRankNotEmpty,
    ValidationYearInRange,
    parse_date_range,
    validate_event_date_notempty,
)

EXAMPLES = Path(__file__).parent / "examples"


class BdqTest(unittest.TestCase):
    def test_bdq_empty_and_notempty_semantics(self):
        for value in [None, "", " \t\n\r", "\x00\x0e\x0f\x1f\x20"]:
            with self.subTest(value=repr(value)):
                response = validate_event_date_notempty(value)
                self.assertEqual(response["Response.status"], "RUN_HAS_RESULT")
                self.assertEqual(response["Response.result"], "NOT_COMPLIANT")
        for value in ["1916-07", "NULL", "unknown", "?", "\u00a0", "not a date"]:
            with self.subTest(value=repr(value)):
                self.assertEqual(validate_event_date_notempty(value)["Response.result"], "COMPLIANT")

    def test_rejects_wrong_input_types(self):
        for value in [1916, False, [], {}]:
            with self.subTest(value=value), self.assertRaises(TypeError):
                validate_event_date_notempty(value)


class AnnotationTest(unittest.TestCase):
    def setUp(self):
        self.job = json.loads((EXAMPLES / "sandbox-job.json").read_text(encoding="utf-8"))

    def test_real_sandbox_snapshot(self):
        original = copy.deepcopy(self.job)
        event = test_bdq_event(self.job)
        self.assertEqual(event["jobId"], self.job["jobId"])
        self.assertEqual(len(event["annotations"]), 1)
        annotation = event["annotations"][0]
        result = json.loads(annotation["oa:hasBody"]["oa:value"][0])
        self.assertEqual(result["input"], {"dwc:eventDate": "1916-07"})
        self.assertEqual(result["Response.result"], "COMPLIANT")
        self.assertEqual(annotation["oa:motivation"], "oa:assessing")
        self.assertEqual(annotation["oa:hasTarget"]["@type"], "ods:DigitalSpecimen")
        self.assertEqual(
            annotation["oa:hasTarget"]["oa:hasSelector"]["ods:term"],
            "$['ods:hasEvents'][0]['dwc:eventDate']",
        )
        self.assertEqual(self.job, original)

    def test_missing_date_selects_existing_event(self):
        job = json.loads((EXAMPLES / "missing-date-job.json").read_text(encoding="utf-8"))
        annotation = test_bdq_event(job)["annotations"][0]
        self.assertEqual(
            annotation["oa:hasTarget"]["oa:hasSelector"],
            {"@type": "ods:ClassSelector", "ods:class": "$['ods:hasEvents'][0]"},
        )
        self.assertEqual(json.loads(annotation["oa:hasBody"]["oa:value"][0])["Response.result"], "NOT_COMPLIANT")

    def test_no_events_selects_whole_specimen(self):
        for events in [[], None]:
            with self.subTest(events=events):
                job = copy.deepcopy(self.job)
                if events is None:
                    del job["object"]["ods:hasEvents"]
                else:
                    job["object"]["ods:hasEvents"] = events
                annotation = test_bdq_event(job)["annotations"][0]
                self.assertNotIn("oa:hasSelector", annotation["oa:hasTarget"])
                result = json.loads(annotation["oa:hasBody"]["oa:value"][0])
                self.assertEqual(result["Response.result"], "NOT_COMPLIANT")

    def test_multiple_events_keep_values_and_paths_separate(self):
        self.job["object"]["ods:hasEvents"] = [
            {"@type": "ods:Event", "dwc:eventDate": "1916-07"},
            {"@type": "ods:Event", "dwc:eventDate": ""},
        ]
        annotations = test_bdq_event(self.job)["annotations"]
        self.assertEqual(len(annotations), 2)
        for index, expected in enumerate(["COMPLIANT", "NOT_COMPLIANT"]):
            result = json.loads(annotations[index]["oa:hasBody"]["oa:value"][0])
            self.assertEqual(result["Response.result"], expected)
            self.assertEqual(result["inputPath"], f"$['ods:hasEvents'][{index}]['dwc:eventDate']")

    def test_rejects_malformed_jobs(self):
        for job in [[], {}, {"jobId": ""}, {"jobId": "x", "object": []}]:
            with self.subTest(job=job), self.assertRaises((TypeError, ValueError)):
                test_bdq_event(job)


class MessageTest(unittest.TestCase):
    def setUp(self):
        self.body = (EXAMPLES / "sandbox-job.json").read_bytes()
        self.channel = Mock()
        self.method = SimpleNamespace(delivery_tag=42)

    @patch("main.mark_job_as_running")
    @patch("main.publish_event")
    def test_acknowledges_after_publishing(self, publish, mark_running):
        publish.side_effect = lambda *args, **kwargs: self.channel.basic_ack.assert_not_called()
        main.process_message(self.channel, self.method, None, self.body)
        mark_running.assert_called_once_with(json.loads(self.body)["jobId"])
        self.assertEqual(publish.call_args.args[0]["jobId"], json.loads(self.body)["jobId"])
        self.channel.basic_ack.assert_called_once_with(delivery_tag=42)

    @patch("main.mark_job_as_running")
    @patch("main.publish_event", side_effect=RuntimeError("broker unavailable"))
    def test_publish_failure_leaves_job_unacknowledged(self, publish, mark_running):
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            main.process_message(self.channel, self.method, None, self.body)
        self.channel.basic_ack.assert_not_called()

    @patch("main.mark_job_as_running")
    @patch("main.publish_event")
    def test_processing_error_publishes_correlated_failure(self, publish, mark_running):
        with self.assertLogs(main.LOGGER, level="ERROR"):
            main.process_message(self.channel, self.method, None, b'{"jobId":"failed-example","object":{}}')
        publish.assert_called_once()
        self.assertTrue(publish.call_args.kwargs["failed"])
        self.assertEqual(publish.call_args.args[0]["jobId"], "failed-example")
        self.assertIn("errorMessage", publish.call_args.args[0])
        self.channel.basic_ack.assert_called_once_with(delivery_tag=42)

    @patch("main.publish_event")
    def test_malformed_message_is_rejected(self, publish):
        with self.assertLogs(main.LOGGER, level="ERROR"):
            main.process_message(self.channel, self.method, None, b"invalid json")
        publish.assert_not_called()
        self.channel.basic_nack.assert_called_once_with(delivery_tag=42, requeue=False)
        self.channel.basic_ack.assert_not_called()


class PluginTest(unittest.TestCase):
    def setUp(self):
        self.job = json.loads((EXAMPLES / "sandbox-job.json").read_text(encoding="utf-8"))

    def tearDown(self):
        BDQEvent.reset_registry()

    def test_custom_bdq_event_subclass_plugin(self):
        class ValidationYearNotEmpty(BDQEvent):
            test_label = "VALIDATION_YEAR_NOTEMPTY"
            test_version_iri = "https://example.org/test/year-notempty"
            term = "dwc:year"

            def validate(self, value: str | None) -> dict[str, str]:
                is_valid = value is not None and value.isdigit()
                return {
                    "Response.status": "RUN_HAS_RESULT",
                    "Response.result": "COMPLIANT" if is_valid else "NOT_COMPLIANT",
                    "Response.comment": "Year is valid digit." if is_valid else "Year is missing or invalid.",
                }

        self.job["object"]["ods:hasEvents"] = [
            {"@type": "ods:Event", "dwc:eventDate": "1916-07", "dwc:year": "1916"},
            {"@type": "ods:Event", "dwc:eventDate": "1920-01"},
        ]

        event = test_bdq_event(self.job, tests=[ValidationEventDateNotEmpty, ValidationYearNotEmpty])
        annotations = event["annotations"]
        self.assertEqual(len(annotations), 4)

        # Event 0: eventDate and year
        res0_date = json.loads(annotations[0]["oa:hasBody"]["oa:value"][0])
        self.assertEqual(res0_date["test"], "VALIDATION_EVENTDATE_NOTEMPTY")
        self.assertEqual(res0_date["Response.result"], "COMPLIANT")
        self.assertEqual(
            annotations[0]["oa:hasTarget"]["oa:hasSelector"]["ods:term"],
            "$['ods:hasEvents'][0]['dwc:eventDate']",
        )

        res0_year = json.loads(annotations[1]["oa:hasBody"]["oa:value"][0])
        self.assertEqual(res0_year["test"], "VALIDATION_YEAR_NOTEMPTY")
        self.assertEqual(res0_year["Response.result"], "COMPLIANT")
        self.assertEqual(
            annotations[1]["oa:hasTarget"]["oa:hasSelector"]["ods:term"],
            "$['ods:hasEvents'][0]['dwc:year']",
        )

        # Event 1: missing year selects class
        res1_date = json.loads(annotations[2]["oa:hasBody"]["oa:value"][0])
        self.assertEqual(res1_date["test"], "VALIDATION_EVENTDATE_NOTEMPTY")
        self.assertEqual(res1_date["Response.result"], "COMPLIANT")

        res1_year = json.loads(annotations[3]["oa:hasBody"]["oa:value"][0])
        self.assertEqual(res1_year["test"], "VALIDATION_YEAR_NOTEMPTY")
        self.assertEqual(res1_year["Response.result"], "NOT_COMPLIANT")
        self.assertEqual(
            annotations[3]["oa:hasTarget"]["oa:hasSelector"],
            {"@type": "ods:ClassSelector", "ods:class": "$['ods:hasEvents'][1]"},
        )

    def test_registered_plugins_and_instances(self):
        class CustomEventTest(BDQEvent):
            test_label = "CUSTOM_EVENT_TEST"
            test_version_iri = "https://example.org/custom-test"
            term = "dwc:eventDate"

            def validate(self, value: str | None) -> dict[str, str]:
                return {
                    "Response.status": "RUN_HAS_RESULT",
                    "Response.result": "COMPLIANT",
                    "Response.comment": "Custom test comment.",
                }

        # Test passing instance
        event_inst = test_bdq_event(self.job, tests=[CustomEventTest()])
        self.assertEqual(len(event_inst["annotations"]), 1)
        res = json.loads(event_inst["annotations"][0]["oa:hasBody"]["oa:value"][0])
        self.assertEqual(res["test"], "CUSTOM_EVENT_TEST")

        # Test registry
        BDQEvent.register(CustomEventTest)
        self.assertIn(CustomEventTest, BDQEvent.get_registered_tests())
        event_reg = test_bdq_event(self.job)
        self.assertEqual(len(event_reg["annotations"]), 2)

        BDQEvent.unregister(CustomEventTest)
        self.assertNotIn(CustomEventTest, BDQEvent.get_registered_tests())
        event_unreg = test_bdq_event(self.job)
        self.assertEqual(len(event_unreg["annotations"]), 1)

    def test_custom_plugin_no_events(self):
        class CustomEventTest(BDQEvent):
            test_label = "CUSTOM_NO_EVENT_TEST"
            test_version_iri = "https://example.org/custom-no-event"
            term = "dwc:verbatimEventDate"

            def validate(self, value: str | None) -> dict[str, str]:
                return {
                    "Response.status": "RUN_HAS_RESULT",
                    "Response.result": "NOT_COMPLIANT",
                    "Response.comment": "No events present.",
                }

        self.job["object"]["ods:hasEvents"] = []
        event = test_bdq_event(self.job, tests=[ValidationEventDateNotEmpty(), CustomEventTest()])
        self.assertEqual(len(event["annotations"]), 2)
        res0 = json.loads(event["annotations"][0]["oa:hasBody"]["oa:value"][0])
        res1 = json.loads(event["annotations"][1]["oa:hasBody"]["oa:value"][0])
        self.assertEqual(res0["test"], "VALIDATION_EVENTDATE_NOTEMPTY")
        self.assertEqual(res1["test"], "CUSTOM_NO_EVENT_TEST")
        self.assertEqual(res1["inputPath"], "$['ods:hasEvents'][*]['dwc:verbatimEventDate']")

    def test_build_mas_annotation_with_custom_test(self):
        class CustomEventTest(BDQEvent):
            test_label = "CUSTOM_DIRECT_TEST"
            test_version_iri = "https://example.org/custom-direct"
            term = "dwc:eventDate"

            def validate(self, value: str | None) -> dict[str, str]:
                return {
                    "Response.status": "RUN_HAS_RESULT",
                    "Response.result": "COMPLIANT",
                    "Response.comment": "Direct annotation built.",
                }

        annotation = build_mas_annotation(
            self.job["object"],
            "1916-07",
            "$['ods:hasEvents'][0]['dwc:eventDate']",
            {"@type": "ods:TermSelector", "ods:term": "$['ods:hasEvents'][0]['dwc:eventDate']"},
            test=CustomEventTest(),
        )
        self.assertEqual(annotation["oa:motivatedBy"], "CUSTOM_DIRECT_TEST: Direct annotation built.")
        result = json.loads(annotation["oa:hasBody"]["oa:value"][0])
        self.assertEqual(result["test"], "CUSTOM_DIRECT_TEST")


TODAY = date(2026, 10, 14)
NOT_MET = ("INTERNAL_PREREQUISITES_NOT_MET", "")
PASS = ("RUN_HAS_RESULT", "COMPLIANT")
FAIL = ("RUN_HAS_RESULT", "NOT_COMPLIANT")


def outcome(response: dict[str, str]) -> tuple[str, str]:
    return response["Response.status"], response["Response.result"]


class DateParsingTest(unittest.TestCase):
    def test_valid_iso_8601_dates(self):
        cases = {
            "1916": (date(1916, 1, 1), date(1916, 12, 31)),
            "1916-07": (date(1916, 7, 1), date(1916, 7, 31)),
            "2000-02-29": (date(2000, 2, 29), date(2000, 2, 29)),
            "20000229": (date(2000, 2, 29), date(2000, 2, 29)),
            "2001-032": (date(2001, 2, 1), date(2001, 2, 1)),
            "1963-03-08T14:07-0600": (date(1963, 3, 8), date(1963, 3, 8)),
            "2009-02-20T08:40Z": (date(2009, 2, 20), date(2009, 2, 20)),
            "2007-11-13/15": (date(2007, 11, 13), date(2007, 11, 15)),
            "2007-11/12": (date(2007, 11, 1), date(2007, 12, 31)),
            "1900/1909": (date(1900, 1, 1), date(1909, 12, 31)),
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(tuple(parse_date_range(value)), expected)

    def test_invalid_iso_8601_dates(self):
        for value in [
            "",
            " 1916",
            "1916-13",
            "2001-02-29",
            "2001-366",
            "0000",
            "1916-7",
            "07/1916",
            "2000-01-05/2000-01-04",
            "1916/1917/1918",
            "2009-W01",
            "1916-07-01T25:00",
            "July 1916",
        ]:
            with self.subTest(value=value):
                self.assertIsNone(parse_date_range(value))


class NewBdqTestsTest(unittest.TestCase):
    def check(self, test, cases):
        for value, expected in cases:
            with self.subTest(test=test.test_label, value=value):
                self.assertEqual(outcome(test.validate(value)), expected)

    def test_eventdate_standard(self):
        self.check(
            ValidationEventDateStandard(),
            [
                (None, NOT_MET),
                (" ", NOT_MET),
                ("1916-07", PASS),
                ("2007-11-13/15", PASS),
                ("2001-02-29", FAIL),
                ("16/07/1916", FAIL),
                (" 1916-07", FAIL),
            ],
        )

    def test_eventdate_inrange(self):
        self.check(
            ValidationEventDateInRange(today=TODAY),
            [
                ("", NOT_MET),
                ("not a date", NOT_MET),
                ("1916-07", PASS),
                ("1582-11-15", PASS),
                ("2026-12-31", PASS),
                ("1582-11-14", FAIL),
                ("1582", FAIL),
                ("2027-01-01", FAIL),
                ("2026-12-30/2027-01-02", FAIL),
            ],
        )
        self.check(ValidationEventDateInRange("1900", "1950"), [("1916-07", PASS), ("1899-12-31", FAIL)])

    def test_year_inrange(self):
        self.check(
            ValidationYearInRange(today=TODAY),
            [
                (None, NOT_MET),
                ("19x6", NOT_MET),
                (1916.5, NOT_MET),
                (True, NOT_MET),
                (1916, PASS),
                ("1916", PASS),
                (1916.0, PASS),
                (1582, PASS),
                (2026, PASS),
                (1581, FAIL),
                ("2027", FAIL),
            ],
        )

    def test_elevation_inrange(self):
        for test in [ValidationMinElevationInRange(), ValidationMaxElevationInRange()]:
            self.check(
                test,
                [
                    (None, NOT_MET),
                    ("high", NOT_MET),
                    ("NaN", NOT_MET),
                    (float("inf"), NOT_MET),
                    (0, PASS),
                    (-430, PASS),
                    ("8850", PASS),
                    (125.5, PASS),
                    (-431, FAIL),
                    ("8850.1", FAIL),
                ],
            )

    def test_countrycode_standard(self):
        self.assertEqual(len(ISO_3166_ALPHA2), 251)
        self.check(
            ValidationCountryCodeStandard(),
            [
                (None, NOT_MET),
                ("", NOT_MET),
                ("BE", PASS),
                ("XZ", PASS),
                ("ZZ", PASS),
                ("be", FAIL),
                (" BE", FAIL),
                ("BEL", FAIL),
                ("UK", FAIL),
                (32, FAIL),
            ],
        )

    def test_basisofrecord_standard(self):
        self.check(
            ValidationBasisOfRecordStandard(),
            [
                (None, NOT_MET),
                ("PreservedSpecimen", PASS),
                ("FossilSpecimen", PASS),
                ("preservedspecimen", FAIL),
                ("PreservedSpecimen ", FAIL),
                ("Specimen", FAIL),
            ],
        )

    def test_coordinates_notzero(self):
        lat, lon = "dwc:decimalLatitude", "dwc:decimalLongitude"
        self.check(
            ValidationCoordinatesNotZero(),
            [
                ({lat: None, lon: 4.4}, NOT_MET),
                ({lat: 51.0}, NOT_MET),
                ({lat: "north", lon: 4.4}, NOT_MET),
                ({lat: 51.026111, lon: 4.478055}, PASS),
                ({lat: 0, lon: 4.4}, PASS),
                ({lat: 0, lon: 0}, FAIL),
                ({lat: "0.0", lon: "0"}, FAIL),
            ],
        )

    def test_dateidentified_inrange(self):
        identified, event_date = "dwc:dateIdentified", "dwc:eventDate"
        self.check(
            ValidationDateIdentifiedInRange(today=TODAY),
            [
                ({identified: None, event_date: "1916-07"}, NOT_MET),
                ({identified: "July 1920", event_date: "1916-07"}, NOT_MET),
                ({identified: "1920", event_date: "16/07/1916"}, NOT_MET),
                ({identified: "1920-05-01", event_date: None}, PASS),
                ({identified: "1920-05-01", event_date: "1916-07"}, PASS),
                ({identified: "1916-07-15", event_date: "1916-07"}, PASS),  # overlaps the event
                ({identified: "1916-06-30", event_date: "1916-07"}, FAIL),  # before the event
                ({identified: "1752-12-31", event_date: None}, FAIL),
                ({identified: "2026-10-15", event_date: None}, FAIL),  # after today
            ],
        )
        without_event = ValidationDateIdentifiedInRange(include_event_date=False, today=TODAY)
        self.check(without_event, [({identified: "1916-06-30", event_date: "not a date"}, PASS)])

    def test_taxon_term_notempty(self):
        # NOTEMPTY tests always run: empty is NOT_COMPLIANT, never INTERNAL_PREREQUISITES_NOT_MET.
        for test in [
            ValidationScientificNameNotEmpty(),
            ValidationKingdomNotEmpty(),
            ValidationTaxonRankNotEmpty(),
            ValidationScientificNameIdNotEmpty(),
            ValidationScientificNameAuthorshipNotEmpty(),
            ValidationNamePublishedInYearNotEmpty(),
        ]:
            self.check(
                test,
                [(None, FAIL), ("", FAIL), (" \t", FAIL), ("Plantae", PASS), ("unknown", PASS), (1753, PASS)],
            )

    def test_taxon_notempty(self):
        self.check(
            ValidationTaxonNotEmpty(),
            [
                ({}, FAIL),
                ({"dwc:scientificName": "", "dwc:kingdom": " "}, FAIL),
                ({"dwc:taxonRank": "SPECIES", "dwc:scientificNameAuthorship": "L."}, FAIL),  # not taxon terms
                ({"dwc:vernacularName": "Sawara cypress"}, PASS),
                ({"dwc:family": "Cupressaceae"}, PASS),
            ],
        )


class NewTestsAnnotationTest(unittest.TestCase):
    def setUp(self):
        self.job = json.loads((EXAMPLES / "sandbox-job.json").read_text(encoding="utf-8"))

    def results(self, job) -> dict[str, list[tuple[dict, dict]]]:
        """Group (annotation target, decoded BDQ result) by test label."""
        grouped: dict[str, list] = {}
        for annotation in test_bdq_event(job, tests=ALL_TESTS)["annotations"]:
            result = json.loads(annotation["oa:hasBody"]["oa:value"][0])
            grouped.setdefault(result["test"], []).append((annotation["oa:hasTarget"], result))
        return grouped

    def test_all_tests_on_real_sandbox_snapshot(self):
        results = self.results(self.job)
        self.assertEqual(set(results), {test.test_label for test in ALL_TESTS})
        expected = {
            "VALIDATION_EVENTDATE_NOTEMPTY": PASS,
            "VALIDATION_EVENTDATE_STANDARD": PASS,
            "VALIDATION_EVENTDATE_INRANGE": PASS,
            "VALIDATION_YEAR_INRANGE": PASS,
            "VALIDATION_MINELEVATION_INRANGE": NOT_MET,
            "VALIDATION_MAXELEVATION_INRANGE": NOT_MET,
            "VALIDATION_COUNTRYCODE_STANDARD": PASS,
            "VALIDATION_COORDINATES_NOTZERO": PASS,
            "VALIDATION_DATEIDENTIFIED_INRANGE": NOT_MET,
            "VALIDATION_BASISOFRECORD_STANDARD": PASS,
            "VALIDATION_SCIENTIFICNAME_NOTEMPTY": PASS,
            "VALIDATION_KINGDOM_NOTEMPTY": PASS,
            "VALIDATION_TAXONRANK_NOTEMPTY": PASS,
            "VALIDATION_SCIENTIFICNAMEID_NOTEMPTY": FAIL,
            "VALIDATION_SCIENTIFICNAMEAUTHORSHIP_NOTEMPTY": PASS,
            "VALIDATION_NAMEPUBLISHEDINYEAR_NOTEMPTY": FAIL,
            "VALIDATION_TAXON_NOTEMPTY": PASS,
        }
        self.assertEqual(set(expected), set(results))
        for label, expected_outcome in expected.items():
            with self.subTest(test=label):
                self.assertEqual(len(results[label]), 1)
                self.assertEqual(outcome(results[label][0][1]), expected_outcome)

    def test_selectors_and_input_paths(self):
        results = self.results(self.job)
        location = "$['ods:hasEvents'][0]['ods:hasLocation']"
        target, result = results["VALIDATION_COUNTRYCODE_STANDARD"][0]
        self.assertEqual(
            target["oa:hasSelector"], {"@type": "ods:TermSelector", "ods:term": f"{location}['dwc:countryCode']"}
        )
        self.assertEqual(result["input"], {"dwc:countryCode": "BE"})
        # Absent elevation: select the location object that should hold it.
        target, result = results["VALIDATION_MINELEVATION_INRANGE"][0]
        self.assertEqual(target["oa:hasSelector"], {"@type": "ods:ClassSelector", "ods:class": location})
        self.assertEqual(result["inputPath"], f"{location}['dwc:minimumElevationInMeters']")
        # Multi-term test: select the georeference object and report both inputs.
        target, result = results["VALIDATION_COORDINATES_NOTZERO"][0]
        self.assertEqual(target["oa:hasSelector"]["ods:class"], f"{location}['ods:hasGeoreference']")
        self.assertEqual(result["input"], {"dwc:decimalLatitude": 51.026111, "dwc:decimalLongitude": 4.478055})
        # Specimen-level term.
        target, result = results["VALIDATION_BASISOFRECORD_STANDARD"][0]
        self.assertEqual(target["oa:hasSelector"]["ods:term"], "$['dwc:basisOfRecord']")
        # Identification without dwc:dateIdentified: select the identification.
        target, result = results["VALIDATION_DATEIDENTIFIED_INRANGE"][0]
        self.assertEqual(target["oa:hasSelector"]["ods:class"], "$['ods:hasIdentifications'][0]")
        self.assertEqual(result["input"]["dwc:eventDate"], "1916-07")
        # Taxon terms live one array deeper, in each taxon identification.
        taxon = "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]"
        target, result = results["VALIDATION_KINGDOM_NOTEMPTY"][0]
        self.assertEqual(target["oa:hasSelector"], {"@type": "ods:TermSelector", "ods:term": f"{taxon}['dwc:kingdom']"})
        self.assertEqual(result["input"], {"dwc:kingdom": "Plantae"})
        target, result = results["VALIDATION_SCIENTIFICNAMEID_NOTEMPTY"][0]
        self.assertEqual(target["oa:hasSelector"], {"@type": "ods:ClassSelector", "ods:class": taxon})
        target, result = results["VALIDATION_TAXON_NOTEMPTY"][0]
        self.assertEqual(target["oa:hasSelector"]["ods:class"], taxon)
        self.assertEqual(len(result["input"]), 25)

    def test_taxon_tests_per_taxon_identification(self):
        identifications = self.job["object"]["ods:hasIdentifications"]
        identifications[0]["ods:hasTaxonIdentifications"].append({"dwc:scientificName": " "})
        identifications.append({"@type": "ods:Identification"})  # no taxon identifications
        entries = self.results(self.job)["VALIDATION_SCIENTIFICNAME_NOTEMPTY"]
        self.assertEqual([outcome(result) for _, result in entries], [PASS, FAIL, FAIL])
        self.assertEqual(
            [target["oa:hasSelector"] for target, _ in entries[1:]],
            [
                # A blank value still exists, so the term itself is selected.
                {
                    "@type": "ods:TermSelector",
                    "ods:term": "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][1]['dwc:scientificName']",
                },
                # No taxon identifications at all: select the identification.
                {"@type": "ods:ClassSelector", "ods:class": "$['ods:hasIdentifications'][1]"},
            ],
        )
        self.assertEqual(
            entries[2][1]["inputPath"],
            "$['ods:hasIdentifications'][1]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
        )

    def test_problem_values_in_specimen(self):
        specimen = self.job["object"]
        specimen["dwc:basisOfRecord"] = "Specimen"
        specimen["ods:hasIdentifications"][0]["dwc:dateIdentified"] = "1915"
        location = specimen["ods:hasEvents"][0]["ods:hasLocation"]
        location["dwc:countryCode"] = "be"
        location["dwc:minimumElevationInMeters"] = -500
        location["dwc:maximumElevationInMeters"] = 9000
        location["ods:hasGeoreference"].update({"dwc:decimalLatitude": 0, "dwc:decimalLongitude": 0})
        results = self.results(self.job)
        for label in [
            "VALIDATION_BASISOFRECORD_STANDARD",
            "VALIDATION_DATEIDENTIFIED_INRANGE",
            "VALIDATION_COUNTRYCODE_STANDARD",
            "VALIDATION_MINELEVATION_INRANGE",
            "VALIDATION_MAXELEVATION_INRANGE",
            "VALIDATION_COORDINATES_NOTZERO",
        ]:
            with self.subTest(test=label):
                self.assertEqual(outcome(results[label][0][1]), FAIL)

    def test_missing_classes_assess_whole_specimen(self):
        specimen = self.job["object"]
        specimen["ods:hasEvents"] = []
        del specimen["ods:hasIdentifications"]
        del specimen["dwc:basisOfRecord"]
        for label, entries in self.results(self.job).items():
            with self.subTest(test=label):
                target, result = entries[0]
                self.assertNotIn("oa:hasSelector", target)
                self.assertNotEqual(outcome(result), PASS)

    def test_library_default_is_registry(self):
        # test_bdq_event() without tests= still uses the registry; main.py passes ALL_TESTS.
        self.assertEqual(len(test_bdq_event(self.job)["annotations"]), 1)

    def test_select_tests(self):
        self.assertEqual(main.select_tests(None), ALL_TESTS)
        self.assertEqual(main.select_tests(" "), ALL_TESTS)
        self.assertEqual(main.select_tests("all"), ALL_TESTS)
        self.assertEqual(
            main.select_tests("VALIDATION_YEAR_INRANGE, VALIDATION_COUNTRYCODE_STANDARD"),
            [ValidationYearInRange, ValidationCountryCodeStandard],
        )
        with self.assertRaisesRegex(ValueError, "VALIDATION_NOPE"):
            main.select_tests("VALIDATION_NOPE")


if __name__ == "__main__":
    unittest.main()
