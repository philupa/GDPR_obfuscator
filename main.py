import json
from src.extractor import extraction_handler
from src.transformer import transformation_handler
import logging

# main function:
# 1. main: takes JSON as argument
# 2. extract: finds file in S3 bucket as required by JSON info
# 3. transform: reformats file to dataframe
# 4. transform: find columns as required by JSON info
# 5. transform: change column contents to '***'
# 6. transform: reformats to original filetype
# 7. main: output


# Setting up logging
def setup_logging():
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('log.txt', mode='w')
    formatter = logging.Formatter('%(asctime)s - %(name)s\
                                  - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class DataTransformer:
    def __init__(self, json_file):
        # loads json input to useable info:
        self.json_loaded = self.load_json(json_file)
        self.s3_url = self.json_loaded['file_to_obfuscate']
        self.pii_fields = self.json_loaded['pii_fields']
        # extracted data:
        self.__data_to_be_transformed = self.__extraction_handler(self.s3_url)

    def load_json(self, json_file):
        help = json.loads(json_file)
        return help

    def __extraction_handler(self, s3_url):
        return extraction_handler(s3_url)

    def _transformation_handler(self, body_data, pii_fields):
        return transformation_handler(body_data, pii_fields)

    def anonymise(self):
        anonymised_data = self._transformation_handler(
            self.__data_to_be_transformed,
            self.pii_fields)
        return anonymised_data


def main(json_file):
    logger = setup_logging()
    try:
        # initialise the data transformer and extract the data
        transformer = DataTransformer(json_file)
        # return anonymised data
        anonymised_data = transformer.anonymise()
        return anonymised_data
    # catch errors
    except Exception as error:
        logger.error('An unexpected error has occurred: %s', error)
        return


if __name__ == '__main__':
    main(
            """{"file_to_obfuscate":
            "s3://my_ingestion_bucket/example/file1.csv",
            "pii_fields": ["name", "email_address"]}"""
        )
