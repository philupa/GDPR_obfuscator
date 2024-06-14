import pandas as pd
import io
from io import StringIO, BytesIO
import logging
import json


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


# TRANSFORMATION FUNCTION
def transformation_handler(data_to_be_transformed, pii_fields):
    logger = setup_logging()
    # Check that pii fields are given, give info if not:
    if len(pii_fields) == 0:
        logger.info('No PII fields given.')
    # Check that the extracted file has data, give error if not:
    if (type(data_to_be_transformed) is str or
       type(data_to_be_transformed) is list or
       type(data_to_be_transformed) is bytes):
        if len(data_to_be_transformed) == 0:
            logger.error('Input file is blank.')
            return
    # CSV to dataframe:
    # Give error if csv is not comma separated:
    if type(data_to_be_transformed) is str:
        if data_to_be_transformed.find(',') == -1:
            logger.error('Comma not used as delimiter in CSV.')
            return
        # CSV to dataframe, assuming comma separation:
        df_with_pii = pd.read_csv(StringIO(data_to_be_transformed), sep=",")
    # JSON to dataframe:
    elif type(data_to_be_transformed) is list:
        df_with_pii = pd.DataFrame.from_dict(data_to_be_transformed)
    # Parquet (assumed in binary) to dataframe:
    elif type(data_to_be_transformed) is bytes:
        buffer = BytesIO(data_to_be_transformed)
        df_with_pii = pd.read_parquet(buffer)
    # If file is none of these data types, give error:
    else:
        logger.error('Unsupported data type.')
        return
    # If dataframe is empty except headers, give error:
    if df_with_pii.empty:
        logger.error('Input data file has no content.')
        return
    # If given PII field was not found in data, give error:
    for item in pii_fields:
        if item not in df_with_pii.columns:
            logger.error(f'PII field "{item}" not found in file.')
    df_anonymised = pd.DataFrame(columns=df_with_pii.columns)
    # Transformation of PII columns to '***':
    for column in df_with_pii.columns:
        if column in pii_fields:
            df_anonymised[column] = ['***'] * len(df_with_pii)
        else:
            df_anonymised[column] = df_with_pii[column]
    # Return dataframe to original format:
    # Dataframe to CSV:
    if type(data_to_be_transformed) is str:
        csv_buffer = io.BytesIO()
        df_anonymised.to_csv(csv_buffer, index=False)
        output = csv_buffer.getvalue()
    # Dataframe to JSON:
    elif type(data_to_be_transformed) is list:
        json_string = df_anonymised.to_json(orient='records')
        output = json.loads(json_string)
    # Dataframe to parquet binary format:
    elif type(data_to_be_transformed) is bytes:
        buffer = BytesIO()
        df_anonymised.to_parquet(buffer, index=False)
        output = buffer.getvalue()
    return output


if __name__ == '__main__':
    csv = """student_id,name,course,cohort,graduation_date,email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'"""

    transformation_handler(csv, ["name", "email_address"])
