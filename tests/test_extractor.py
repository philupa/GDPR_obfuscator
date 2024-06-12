import unittest
from unittest.mock import patch, MagicMock
from src.extractor import (
    extraction_handler,
    get_data)
import os
from botocore.exceptions import ClientError
import json
import pandas as pd
from io import BytesIO, StringIO

test_s3_url = "s3://my_ingestion_bucket/new_data/file1.csv"
test_s3_bucket = "my_ingestion_bucket"
test_s3_filepath = "new_data/file1.csv"
test_csv = """student_id,name,course,cohort,graduation_date,email_address\n
1234,'John Smith','Software','2024-03-31','j.smith@email.com'
"""
test_json_body = {
    "student_id": 1234,
    "name": "John Smith",
    "course": "Software",
    "cohort": "Jan",
    "graduation_date": "2024-03-31",
    "email_address": "j.smith@email.com"
                 }


class TestExtractorFunctionality(unittest.TestCase):
    @patch('src.extractor.get_data')
    @patch('src.extractor.get_client')
    def test_extractor_calls_correctly_for_csvs(
                        self,
                        mock_get_client, mock_get_data,
                        ):
        """Test checks that the main extract function calls all of the
        functions correctly and with the correct returned parameters"""
        extraction_handler(test_s3_url)

        mock_get_client.assert_called_once()
        mock_get_data.assert_called_once_with(mock_get_client.return_value,
                                              test_s3_bucket, test_s3_filepath)

    @patch('src.extractor.get_data')
    @patch('src.extractor.get_client')
    def test_extractor_calls_correctly_for_longer_filepaths(
                        self,
                        mock_get_client, mock_get_data,
                        ):
        """Test checks that the main extract function works on other
        filepaths"""
        test_longer_s3_url = """s3://my_ingestion_bucket/new_folder/\
unneeded_folder/new_data/file1.csv"""
        test_longer_filepath = "new_folder/unneeded_folder/new_data/file1.csv"
        extraction_handler(test_longer_s3_url)
        mock_get_data.assert_called_once_with(mock_get_client.return_value,
                                              test_s3_bucket,
                                              test_longer_filepath)

    @patch('src.extractor.get_data')
    @patch('src.extractor.get_client')
    def test_extractor_calls_correctly_for_jsons(
            self,
            mock_get_client, mock_get_data,
            ):
        json_url = "s3://my_ingestion_bucket/new_data/file1.json"
        json_filepath = "new_data/file1.json"
        extraction_handler(json_url)
        mock_get_data.assert_called_once_with(mock_get_client.return_value,
                                              test_s3_bucket,
                                              json_filepath)

    @patch('src.extractor.get_data')
    @patch('src.extractor.get_client')
    def test_extractor_calls_correctly_for_parquets(
            self,
            mock_get_client, mock_get_data,
            ):
        parquet_url = "s3://my_ingestion_bucket/new_data/file1.parquet"
        parquet_filepath = "new_data/file1.parquet"
        pqt_url = "s3://my_ingestion_bucket/new_data/file1.parquet"
        pqt_filepath = "new_data/file1.parquet"
        extraction_handler(parquet_url)
        mock_get_data.assert_called_with(mock_get_client.return_value,
                                         test_s3_bucket,
                                         parquet_filepath)
        extraction_handler(pqt_url)
        mock_get_data.assert_called_with(mock_get_client.return_value,
                                         test_s3_bucket,
                                         pqt_filepath)


class TestSubFunctions(unittest.TestCase):
    @patch('src.extractor.boto3.client')
    def test_csv_url_is_read_correctly(self, mock_boto_client):
        # Set up the mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        # Create a mock response body
        mock_body = MagicMock()
        mock_body.read.return_value = test_csv.encode('utf-8')
        # Mock the response from S3
        mock_s3_client.get_object.return_value = {
            'Body': mock_body
        }
        result = get_data(mock_s3_client, "my_ingestion_bucket",
                          'new_data/file1.csv')
        self.assertEqual(result, test_csv)

    @patch('src.extractor.boto3.client')
    def test_json_url_is_read_correctly(self, mock_boto_client):
        # Set up the mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        # Create a mock response body
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(
            test_json_body).encode('utf-8')
        # Mock the response from S3
        mock_s3_client.get_object.return_value = {
            'Body': mock_body
        }
        result = get_data(mock_s3_client, "my_ingestion_bucket",
                          'new_data/file1.json')
        self.assertEqual(result, test_json_body)

    @patch('src.extractor.boto3.client')
    def test_parquet_url_is_read_correctly(self, mock_boto_client):
        # parquet format file body:
        df = pd.read_csv(StringIO(test_csv))
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        # Set up the mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        # Create a mock response body
        mock_body = MagicMock()
        mock_body.read.return_value = buffer.getvalue()
        # Mock the response from S3
        mock_s3_client.get_object.return_value = {
            'Body': mock_body
        }
        result = get_data(mock_s3_client, "my_ingestion_bucket",
                          'new_data/file1.pqt')
        self.assertEqual(result, buffer.getvalue())


class TestExtractorErrorHandling(unittest.TestCase):

    @patch('src.extractor.get_client')
    def test_non_correct_file_types_raise_TypeError(self, mock_get_client):
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client
        test_non_csv_url = "s3://my_ingestion_bucket/new_data/file1.jpg"
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        extraction_handler(test_non_csv_url)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        self.assertIn("File is not csv or json or parquet format.",
                      log_contents)

    @patch('src.extractor.get_client')
    def test_non_files_raise_TypeError(self, mock_get_client):
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client
        test_non_string = 1
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        extraction_handler(test_non_string)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        self.assertIn("File path not given correctly.", log_contents)

    @patch('src.extractor.get_client')
    def test_exception_error(self, mock_get_client):
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client
        mock_get_client.side_effect = Exception
        test_non_string = test_s3_url
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        extraction_handler(test_non_string)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        self.assertIn("An unexpected error has occurred:", log_contents)

    @patch('src.extractor.get_client')
    def test_internal_service_error(self, mock_get_client):
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client
        mock_get_client.side_effect = ClientError(
            error_response={"Error": {"Code": "InternalServiceError"}},
            operation_name="ClientError"
        )
        test_non_string = test_s3_url
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        extraction_handler(test_non_string)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        self.assertIn("Internal service error detected.", log_contents)

    @patch('src.extractor.get_client')
    def test_bucket_not_found(self, mock_get_client):
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client
        mock_get_client.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchBucket"}},
            operation_name="ClientError"
        )
        test_non_string = test_s3_url
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        extraction_handler(test_non_string)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        self.assertIn("Bucket not found.", log_contents)

    @patch('src.extractor.get_client')
    def test_file_not_found(self, mock_get_client):
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client
        mock_get_client.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchKey"}},
            operation_name="ClientError"
        )
        test_non_string = test_s3_url
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        extraction_handler(test_non_string)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        self.assertIn("File not found.", log_contents)
