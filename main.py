import json
from src.extractor import extraction_handler
from src.transformer import transformation_handler

# main function:
# takes JSON as argument
# finds file in S3 bucket as required by JSON info
# reformats file to dataframe
# find columns as required by JSON info
# change column contents to '***'
# reformats to original filetype
# output


class DataTransformer:
    def __init__(self, json_file):
        self.json_loaded = self.load_json(json_file)
        self.s3_url = self.json_loaded['file_to_obfuscate']
        self.pii_fields = self.json_loaded['pii_fields']
        self._csv_to_be_transformed = self._extraction_handler(self.s3_url)

    def load_json(self, json_file):
        return json.loads(json_file)

    def _extraction_handler(self, s3_url):
        return extraction_handler(s3_url)

    def _transformation_handler(self, body_data, pii_fields):
        return transformation_handler(body_data, pii_fields)

    def anonymise(self):
        anonymised_csv = self._transformation_handler(
            self._csv_to_be_transformed,
            self.pii_fields)
        return anonymised_csv


def main(json_file):
    transformer = DataTransformer(json_file)
    anonymised_csv = transformer.anonymise()
    return anonymised_csv


if __name__ == '__main__':
    main(
            """{"file_to_obfuscate":
            "s3://my_ingestion_bucket/new_data/file1.csv",
            "pii_fields": ["name", "email_address"]}"""
        )
