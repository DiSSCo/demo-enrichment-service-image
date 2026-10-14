"""Offline checks for BDQ semantics, specimen mapping, and message handling."""

import copy
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main
from annotations import test_bdq_event
from bdq import validate_event_date_notempty

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


if __name__ == "__main__":
    unittest.main()
