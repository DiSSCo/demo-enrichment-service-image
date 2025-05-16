import unittest

from main import find_fuzzy_match, get_json_path
from shared import Motivation


class TestFindMatches(unittest.TestCase):
    def setUp(self):
        # Sample specimen data for testing
        self.specimen = {
            "ods:hasIdentifications": [
                {
                    "ods:hasTaxonIdentifications": [
                        {"dwc:scientificName": "Homo sapiens", "dwc:taxonomicStatus": "ACCEPTED"}
                    ],
                    "ods:hasAgents": [
                        {"ods:hasRoles": ["contributor"], "schema:name": "Alice"},
                        {"ods:hasRoles": ["identifier"], "schema:name": "Bob"},
                    ],
                },
                {
                    "ods:hasTaxonIdentifications": [
                        {"dwc:scientificName": "Homo sapiens sapiens", "dwc:taxonomicStatus": "SYNONYM"}
                    ],
                    "ods:hasAgents": [
                        {"ods:hasRoles": ["contributor"], "schema:name": "Cody"},
                        {"ods:hasRoles": ["identifier"], "schema:name": "Dave"},
                    ],
                },
            ],
            "ods:hasEvents": [
                {"dwc:locality": "New York City", "dwc:verbatimLocality": "NYC"},
                {"dwc:locality": "New York", "dwc:verbatimLocality": "New York State"},
            ],
        }

    def test_no_matches(self):
        """Test when no matches are found"""
        result_value = "Pan troglodytes"
        paths = [
            "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']",
            "$['ods:hasIdentifications'][1]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']",
        ]
        match_path, motivation = find_fuzzy_match(
            self.specimen,
            paths,
            result_value,
        )
        self.assertEqual(match_path, "")
        self.assertEqual(motivation, Motivation.ADDING.value)

    def test_single_match(self):
        """Test when exactly one match is found"""
        result_value = "Homo sapiens"
        paths = [
            "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']",
            "$['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][1]['dwc:scientificName']",
        ]
        match_path, motivation = find_fuzzy_match(
            self.specimen,
            paths,
            result_value,
        )
        self.assertEqual(match_path, paths[0])
        self.assertEqual(motivation, Motivation.ASSESSING.value)

    def test_fuzzy_matching(self):
        """Test fuzzy matching with similar but not identical values"""
        result_value = "New York City, USA"
        paths = ["$['ods:hasEvents'][0]['dwc:locality']", "$['ods:hasEvents'][1]['dwc:locality']"]
        match_path, motivation = find_fuzzy_match(self.specimen, paths, result_value)
        self.assertEqual(match_path, paths[0])
        self.assertEqual(motivation, Motivation.EDITING.value)

    def test_multiple_matches_above_threshold(self):
        """Test when multiple matches are found above the similarity threshold"""
        result_value = "New York C"
        paths = ["$['ods:hasEvents'][0]['dwc:locality']", "$['ods:hasEvents'][1]['dwc:locality']"]
        match_path, motivation = find_fuzzy_match(self.specimen, paths, result_value)
        self.assertEqual(match_path, paths[0])
        self.assertEqual(motivation, Motivation.EDITING.value)

    def test_filter_value(self):
        """Test filtering with a specific value"""
        expected = [
            "['ods:hasIdentifications'][0]['ods:hasAgents'][1]['schema:name']",
            "['ods:hasIdentifications'][1]['ods:hasAgents'][1]['schema:name']",
        ]
        paths = get_json_path(self.specimen, "dwc:identifiedBy", True)
        self.assertEqual(paths, expected)


if __name__ == "__main__":
    unittest.main()
