from main import (load_json,
                  main)
import unittest
from unittest.mock import patch
import json


class TestSubFunctions():
    def test_json_is_loaded_correctly(self):
        test_json = """{
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
        "pii_fields": ["name", "email_address"]
        }"""
        test_s3_url = "s3://my_ingestion_bucket/new_data/file1.csv"
        result = load_json(test_json)
        assert result['file_to_obfuscate'] == test_s3_url
        assert result["pii_fields"] == ["name", "email_address"]


class TestMainFunction(unittest.TestCase):
    @patch('main.transformation_handler')
    @patch('main.extraction_handler')
    @patch('main.load_json')
    def test_main_functions_correctly(
         self,
         mock_load_json,
         mock_extraction_handler,
         mock_trnfrm_handler):
        test_json = """{
            "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
            "pii_fields": ["name", "email_address"]
            }"""
        test_url = "s3://my_ingestion_bucket/new_data/file1.csv"
        test_csv = """student_id,name,course,cohort,graduation_date,\
            email_address
            1234,'John Smith','Software','August','2024-03-31',\
                'j.smith@email.com'"""
        test_pii_fields = ["name", "email_address"]
        mock_load_json.return_value = json.loads(test_json)
        mock_extraction_handler.return_value = test_csv

        main(test_json)
        mock_load_json.assert_called_once_with(test_json)
        mock_extraction_handler.assert_called_once_with(test_url)
        mock_trnfrm_handler.assert_called_once_with(test_csv, test_pii_fields)
