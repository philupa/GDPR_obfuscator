from src.transformer import transformation_handler
import os
import pandas as pd
from io import BytesIO, StringIO

test_csv = """student_id,name,course,cohort,graduation_date,\
email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'"""
test_s3_url = "s3://my_ingestion_bucket/new_data/file1.csv"
test_json_url = "s3://my_ingestion_bucket/new_data/file1.json"


class TestTransformerFunctionality():
    def test_transformer_works_with_one_row(self):
        test_pii_fields = ["name", "email_address"]
        test_result = b"""student_id,name,course,cohort,graduation_date,\
email_address\n1234,***,'Software','August','2024-03-31',***\n"""
        actual = transformation_handler(test_csv, test_pii_fields)
        assert actual == test_result

    def test_with_multiple_rows(self):
        test_csv = """student_id,name,course,cohort,graduation_date,\
email_address\n\
1234,'John Smith','Software','August','2024-03-31','j.smith@email.com'\n\
1235,'Joe Smith','Data','November','2024-03-31','j.smith@email.com'"""
        test_pii_fields = ["student_id", "course"]
        test_result = b"""student_id,name,course,cohort,graduation_date,\
email_address\n\
***,'John Smith',***,'August','2024-03-31','j.smith@email.com'\n\
***,'Joe Smith',***,'November','2024-03-31','j.smith@email.com'\n"""

        actual = transformation_handler(test_csv, test_pii_fields)
        assert actual == test_result

    def test_transformer_works_with_json(self):
        test_json = [{
            "student_id": 1234,
            "name": "John Smith",
            "course": "Software",
            "cohort": "Jan",
            "graduation_date": "2024-03-31",
            "email_address": "j.smith@email.com"
        }]
        test_pii_fields = ["name", "email_address"]
        actual = transformation_handler(test_json, test_pii_fields)
        test_result_json = [{
            "student_id": 1234,
            "name": "***",
            "course": "Software",
            "cohort": "Jan",
            "graduation_date": "2024-03-31",
            "email_address": "***"
        }]
        assert actual == test_result_json

    def test_transformer_works_with_json_with_multiples(self):
        test_json = [{
            "student_id": 1234,
            "name": "John Smith",
            "course": "Software",
            "cohort": "Jan",
            "graduation_date": "2024-03-31",
            "email_address": "j.smith@email.com"
        },
                     {
            "student_id": 1235,
            "name": "Joe Smith",
            "course": "Data",
            "cohort": "Jan",
            "graduation_date": "2024-03-31",
            "email_address": "j.smith2@email.com"
        }]
        test_pii_fields = ["name", "email_address"]
        actual = transformation_handler(test_json, test_pii_fields)
        test_result_json = [{
            "student_id": 1234,
            "name": "***",
            "course": "Software",
            "cohort": "Jan",
            "graduation_date": "2024-03-31",
            "email_address": "***"
        },
                            {
            "student_id": 1235,
            "name": "***",
            "course": "Data",
            "cohort": "Jan",
            "graduation_date": "2024-03-31",
            "email_address": "***"
        }]
        assert actual == test_result_json

    def test_transformer_works_with_parquet(self):
        df_original = pd.read_csv(StringIO(test_csv))
        buffer_original = BytesIO()
        df_original.to_parquet(buffer_original, index=False, engine='pyarrow')
        buffer_original.seek(0)
        test_pii_fields = ["name", "email_address"]
        # expected dataframe:
        expected_csv = """student_id,name,course,cohort,graduation_date,\
email_address\n1234,***,'Software','August','2024-03-31',***\n"""
        df_expected = pd.read_csv(StringIO(expected_csv))
        transformed_data = transformation_handler(buffer_original.getvalue(),
                                                  test_pii_fields)
        buffer_transformed = BytesIO(transformed_data)
        df_transformed = pd.read_parquet(buffer_transformed)
        csv_buffer = StringIO()
        df_transformed.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        df_transformed_csv = pd.read_csv(csv_buffer)

        pd.testing.assert_frame_equal(df_expected, df_transformed_csv)


class TestSecurityVulnerabilities():
    def test_transformer_input_is_not_mutated(self):
        test_pii_fields = ["name", "email_address"]
        transformation_handler(test_csv, test_pii_fields)
        assert test_csv == """student_id,name,course,cohort,graduation_date,\
email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'"""

    def test_transformer_creates_new_csv_data(self):
        test_csv = """student_id,name,course,cohort,graduation_date,\
email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'
"""
        test_pii_fields = []
        output = transformation_handler(test_csv, test_pii_fields)
        assert test_csv == output.decode('utf-8')
        assert test_csv is not output.decode('utf-8')


class TestErrorHandling():
    def test_pii_field_not_found_error(self):
        test_pii_fields = ['location']
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'PII field "location" not found in file.'
        assert log_message in log_contents

    def test_csv_file_blank(self):
        test_csv = ""
        test_pii_fields = ["name", "email_address"]
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'Input file is blank.'
        assert log_message in log_contents

    def test_csv_file_has_has_no_content(self):
        test_csv = """student_id,name,course,cohort,graduation_date,\
email_address"""
        test_pii_fields = ["name", "email_address"]
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'Input data file has no content.'
        assert log_message in log_contents

    def test_csv_file_is_not_comma_delimited(self):
        test_csv = """student_id;name;course;cohort;graduation_date;\
email_address"""
        test_pii_fields = ["name", "email_address"]
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'Comma not used as delimiter in CSV.'
        assert log_message in log_contents

    def test_data_file_is_not_correct_type(self):
        test_data = 1
        test_pii_fields = ["name", "email_address"]
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        transformation_handler(test_data, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'Unsupported data type.'
        assert log_message in log_contents

    def test_no_pii_fields_given(self):
        test_pii_fields = []
        if os.path.exists('log.txt'):
            os.remove('log.txt')
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'No PII fields given.'
        assert log_message in log_contents
