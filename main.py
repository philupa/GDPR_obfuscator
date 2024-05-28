import json
from src.extractor import extraction_handler
from src.transformer import transformation_handler

# main function:
# takes JSON as argument
# finds file in S3 from JSON
# reformat to parquet
# find columns reqd in JSON
# change column contents to '***'
# reformat to CSV
# output string


def load_json(json_file):
    return json.loads(json_file)

# MAIN FUNCTION


def main(json_file):
    json_loaded = load_json(json_file)
    s3_url = json_loaded['file_to_obfuscate']
    pii_fields = json_loaded['pii_fields']
    csv_to_be_transformed = extraction_handler(s3_url)
    anonymised_csv = transformation_handler(csv_to_be_transformed, pii_fields)
    return anonymised_csv


if __name__ == '__main__':
    main(
            """{"file_to_obfuscate":
            "s3://my_ingestion_bucket/new_data/file1.csv",
            "pii_fields": ["name", "email_address"]}"""
        )
