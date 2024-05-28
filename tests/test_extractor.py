import unittest
import pytest
import boto3
from moto import mock_secretsmanager
import os
from unittest.mock import Mock, patch
import json
from io import BytesIO
from src.extractor import (
    extraction_handler,
    get_csv,
    get_credentials)


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def secrets(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


test_s3_url = "s3://my_ingestion_bucket/new_data/file1.csv"


class TestExtractorFunctionality(unittest.TestCase):
    @patch('src.extractor.get_csv')
    @patch('src.extractor.get_con')
    @patch('src.extractor.get_credentials', autospec=True)
    def test_extractor_calls_functions_correctly(
                        self,
                        mock_get_credentials,
                        mock_get_con, mock_get_csv,
                        ):
        """Test checks that the main extract function calls all of the util
        functions correctly and with the correct returned parameters"""
        extraction_handler(test_s3_url)
        mock_get_credentials.assert_called_once_with("S3Credentials")
        mock_get_con.assert_called_once_with(mock_get_credentials.return_value)
        mock_get_csv.assert_called_once_with(mock_get_con.return_value,
                                             test_s3_url)


class TestSubFunctions(unittest.TestCase):
    @patch('src.extractor.boto3.client')
    def test_csv_url_is_read_correctly(self, mock_boto_client):
        test_url = "s3://my_ingestion_bucket/new_data/file1.csv"
        test_csv = """student_id,name,course,cohort,graduation_date,
                    email_address\n
                    1234,'John Smith','Software','2024-03-31',
                    'j.smith@email.com'
                    """
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.return_value = {
            'Body': BytesIO(test_csv.encode('utf-8'))
        }
        result = get_csv(mock_s3_client, test_url)
        self.assertEqual(result, test_csv)


class TestGetCredentials:
    def test_get_credentials(self, secrets):
        """
        Checks to see that get_credentials() is able to connect to
        the AWS secrets manager and return the correct credentials
        to connect to OLTP DB
        """
        secret_id = "test_secret"
        secret_values = {
            "engine": "postgres",
            "username": "test_user",
            "password": "test_password",
            "host": "test-database.us-west-2.rds.amazonaws.com",
            "dbname": "test-database",
            "port": "2222",
        }
        secrets.create_secret(Name=secret_id,
                              SecretString=json.dumps(secret_values))
        output = get_credentials(secret_id)
        assert output == secret_values
