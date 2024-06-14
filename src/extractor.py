import boto3
import logging
from botocore.exceptions import ClientError
import json
from io import BytesIO


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

# Connect to S3 client:
def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


# Read data from S3 bucket:
def get_data(client, target_bucket, filepath):
    response = client.get_object(
        Bucket=target_bucket,
        Key=filepath)
    # Read CSV files:
    if filepath[-4:] == '.csv':
        data_body = response['Body'].read().decode('utf-8')
    # Read JSON files:
    elif filepath[-5:] == '.json':
        data_body = json.loads(response['Body'].read())
    # Read parquet binary format files:
    elif filepath[-4:] == '.pqt' or filepath[-8:] == '.parquet':
        buffer = BytesIO(response['Body'].read())
        data_body = buffer.getvalue()
    return data_body


def extraction_handler(s3_url):
    logger = setup_logging()
    try:
        if type(s3_url) is str:
            # Connect to S3 client:
            client = get_client()
            # Create required information from URL:
            s3_bucket = s3_url.split('/')[2]
            s3_filepath = '/'.join(s3_url.split('/')[3:])
            # Check file extension is expected, if so, read the data:
            if (s3_url[-4:] == '.csv' or s3_url[-5:] == '.json' or
               s3_url[-4:] == '.pqt' or s3_url[-8:] == '.parquet'):
                data_to_be_transformed = get_data(
                    client, s3_bucket, s3_filepath)
                return data_to_be_transformed
            # If not expected extension, return error:
            else:
                logger.error("File is not csv or json or parquet format.")
                return
        # if file path is not string, return error:
        else:
            logger.error("File path not given correctly.")
            return
    except ClientError as err:
        # If there is an error with the S3 client:
        if err.response["Error"]["Code"] == "InternalServiceError":
            logger.error("Internal service error detected.")
        # If the bucket does not exist:
        if err.response["Error"]["Code"] == "NoSuchBucket":
            logger.error("Bucket not found.")
        # If the file does not exist:
        if err.response["Error"]["Code"] == "NoSuchKey":
            logger.error("File not found.")
    # All other errors:
    except Exception as err:
        logger.error(f"An unexpected error has occurred: {str(err)}")
        return err
