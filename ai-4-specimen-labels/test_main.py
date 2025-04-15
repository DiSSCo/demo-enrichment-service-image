import unittest
from .main import find_match
from typing import Dict, Any

class TestFindMatches(unittest.TestCase):
    def setUp(self):
        # Sample specimen data for testing
        self.specimen = {
            "ods:hasIdentifications": [
                {
                    "ods:hasTaxonIdentifications": [
                        {
                            "dwc:scientificName": "Homo sapiens",
                            "dwc:taxonomicStatus": "ACCEPTED"
                        },
                        {
                            "dwc:scientificName": "Homo sapiens sapiens",
                            "dwc:taxonomicStatus": "SYNONYM"
                        }
                    ]
                }
            ],
            "ods:hasEvents": [
                {
                    "dwc:locality": "New York City",
                    "dwc:verbatimLocality": "NYC"
                },
                {
                    "dwc:locality": "New York",
                    "dwc:verbatimLocality": "New York State"
                }
            ]
        }

    def test_no_matches(self):
        """Test when no matches are found"""
        results = {"dwc:scientificName": "Pan troglodytes"}
        matches = find_match(
            self.specimen,
            "dwc:scientificName",
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
            results
        )
        self.assertEqual(matches, [])

    def test_single_match(self):
        """Test when exactly one match is found"""
        results = {"dwc:scientificName": "Homo sapiens"}
        matches = find_match(
            self.specimen,
            "dwc:scientificName",
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
            results
        )
        self.assertEqual(len(matches), 1)
        self.assertIn("['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:scientificName']", matches[0])

    def test_fuzzy_matching(self):
        """Test fuzzy matching with similar but not identical values"""
        results = {"dwc:locality": "New York City, USA"}
        matches = find_match(
            self.specimen,
            "dwc:locality",
            "$['ods:hasEvents'][*]['dwc:locality']",
            results
        )
        self.assertEqual(len(matches), 1)
        self.assertIn("['ods:hasEvents'][0]['dwc:locality']", matches[0])

    def test_multiple_matches_above_threshold(self):
        """Test when multiple matches are found above the similarity threshold"""
        results = {"dwc:locality": "New York"}
        matches = find_match(
            self.specimen,
            "dwc:locality",
            "$['ods:hasEvents'][*]['dwc:locality']",
            results
        )
        self.assertTrue(len(matches) >= 1)

    def test_case_insensitivity(self):
        """Test that matching is case insensitive"""
        results = {"dwc:scientificName": "HOMO SAPIENS"}
        matches = find_match(
            self.specimen,
            "dwc:scientificName",
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:scientificName']",
            results
        )
        self.assertEqual(len(matches), 1)

    def test_partial_matches(self):
        """Test partial matching with different word orders"""
        results = {"dwc:locality": "City of New York"}
        matches = find_match(
            self.specimen,
            "dwc:locality",
            "$['ods:hasEvents'][*]['dwc:locality']",
            results
        )
        self.assertEqual(len(matches), 1)
        # import pdb; pdb.set_trace()
        self.assertIn("['ods:hasEvents'][1]['dwc:locality']", matches[0])

    def test_filter_value(self):
        """Test filtering with a specific value"""
        matches = find_match(
            self.specimen,
            "dwc:taxonomicStatus",
            "$['ods:hasIdentifications'][*]['ods:hasTaxonIdentifications'][*]['dwc:taxonomicStatus']",
            {},
            "ACCEPTED"
        )
        self.assertEqual(len(matches), 1)
        self.assertIn("['ods:hasIdentifications'][0]['ods:hasTaxonIdentifications'][0]['dwc:taxonomicStatus']", matches[0])

if __name__ == '__main__':
    unittest.main() 