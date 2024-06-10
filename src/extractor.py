import boto3
import logging
from botocore.exceptions import ClientError


# Setting up logging
def setup_logging():
    logger = logging.getLogger()
    # Remove all handlers associated with the root logger:
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('log.txt', mode='w')
    formatter = logging.Formatter('%(asctime)s - %(name)s\
                                  - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


# EXTRACTION FUNCTIONS

def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


def get_csv(client, target_bucket, filepath):
    response = client.get_object(
        Bucket=target_bucket,
        Key=filepath)
    csv_data = response['Body'].read().decode('utf-8')
    return csv_data


def extraction_handler(s3_url):
    logger = setup_logging()
    try:
        if s3_url[-4:] != '.csv':
            logger.error("file is not csv format")
            return
        client = get_client()
        s3_bucket = s3_url.split('/')[2]
        s3_filepath = '/'.join(s3_url.split('/')[3:])
        csv_to_be_transformed = get_csv(client, s3_bucket, s3_filepath)
        return csv_to_be_transformed
    except ClientError as err:
        if err.response["Error"]["Code"] == "InternalServiceError":
            logger.error("Internal service error detected.")
        if err.response["Error"]["Code"] == "NoSuchBucket":
            logger.error("Bucket not found.")
    except TypeError:
        logger.error("file path not given correctly")
        return
    except Exception as err:
        logger.error(f"An unexpected error has occurred: {str(err)}")
        return err
