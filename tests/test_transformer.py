from src.transformer import transformation_handler
import unittest
import pytest
import re


class TestTransformerFunctionality():
    def test_transformer_works_with_one_row(self):
        test_csv = """student_id,name,course,cohort,graduation_date,email_address
                1234,'John Smith','Software','August','2024-03-31','j.smith@email.com'"""
        test_pii_fields = ["name", "email_address"]
        test_result = b"""student_id,name,course,cohort,graduation_date,email_address\n1234,***,'Software','August','2024-03-31',***\n"""
        
        actual = transformation_handler(test_csv, test_pii_fields)
        assert actual == test_result

    def test_with_multiple_rows(self):
        test_csv = """student_id,name,course,cohort,graduation_date,email_address
                1234,'John Smith','Software','August','2024-03-31','j.smith@email.com'
                1235,'Joe Smith','Data','November','2024-03-31','j.smith@email.com'"""
        test_pii_fields = ["student_id", "course"]
        test_result = b"""student_id,name,course,cohort,graduation_date,email_address\n***,'John Smith',***,'August','2024-03-31','j.smith@email.com'\n***,'Joe Smith',***,'November','2024-03-31','j.smith@email.com'\n"""
        actual = transformation_handler(test_csv, test_pii_fields)
        assert actual == test_result
