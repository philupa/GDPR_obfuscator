

# main function:
# takes JSON as argument
# finds file in S3 from JSON
# reformat to parquet
# find columns reqd in JSON
# change column contents to '***'
# reformat to CSV
# output string

import boto3
import json
from pg8000.native import Connection
from urllib.parse import urlparse
import os
import pandas as pd
import io
from io import StringIO

# client = boto3.client('s3')




# def load_json_from_local_path(json_path):
#     with open(json_path, 'r') as f:
#         return json.load(f)


#EXTRACTION FUNCTIONS

def get_credentials(secret_name):
    """
    Gets credentials from AWS Secrets Manager using
    a specified secret name which will need to be
    set up manually with a JSON object with your own
    credentials in order to access the S3 bucket.
    Required saved structure for example is:
        {
            "username": "YOUR_USERNAME",
            "password": "YOUR_PASSWORD",
            "engine": "YOUR_DB_ENGINE",
            "port": "1234"
            "host": "YOUR_HOST",
            "dbname": "YOUR_DB_NAME"
        }
    Returns a python dictionary structure that is used in
    getting the connection to the database
    Credentials for this project include:
        - username
        - host
        - database name (as dbname)
        - password
    """
    client = boto3.client("secretsmanager", region_name="eu-west-2")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_con(credentials):
    """
    Gets the connection to a database using the returned
    credentials obtained.
    Uses pg8000 to establish the connection.
    Returns an instance of the pg8000 connection class.
    """
    return Connection(
        user=credentials["username"],
        host=credentials["host"],
        database=credentials["dbname"],
        password=credentials["password"],
    )


def get_csv(con, url):
    url = urlparse(url)
    s3_bucket = url.netloc
    s3_path = url.path
    response = con.get_object(
          Bucket=s3_bucket,
          Key=s3_path
     )
    csv_string = response['Body'].read().decode('utf-8')
    return csv_string


def extraction_handler(s3_url):
    # print(os.getcwd())
    # print(load_json_from_local_path('tests/data/test_json.json'))
    
    credentials = get_credentials("S3Credentials")
    con = get_con(credentials)
    csv_to_be_transformed = get_csv(con, s3_url)
    return csv_to_be_transformed







if __name__ == '__main__': 
    extraction_handler("s3://my_ingestion_bucket/new_data/file1.csv")
    