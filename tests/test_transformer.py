from src.transformer import transformation_handler
import os


test_csv = """student_id,name,course,cohort,graduation_date,\
email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'"""


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
        log_message = 'pii field "location" not found in file'
        assert log_message in log_contents

    def test_csv_file_blank(self):
        test_csv = ""
        test_pii_fields = ["name", "email_address"]
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'csv file is blank'
        assert log_message in log_contents

    def test_csv_file_has_has_no_content(self):
        test_csv = """student_id,name,course,cohort,graduation_date,\
email_address"""
        test_pii_fields = ["name", "email_address"]
        transformation_handler(test_csv, test_pii_fields)
        with open('log.txt', 'r') as log_file:
            log_contents = log_file.read()
        log_message = 'csv file has no content'
        assert log_message in log_contents
