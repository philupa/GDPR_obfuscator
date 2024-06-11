import pandas as pd
import io
from io import StringIO
import logging
import json


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


# TRANSFORMATION FUNCTION
def transformation_handler(data_to_be_transformed, pii_fields):
    logger = setup_logging()
    if len(pii_fields) == 0:
        logger.info('No PII fields given.')
    if type(data_to_be_transformed) is str or type(data_to_be_transformed) is list:
        if len(data_to_be_transformed) == 0:
            logger.error('Input file is blank.')
            return
    if type(data_to_be_transformed) is str:
        df_with_pii = pd.read_csv(StringIO(data_to_be_transformed), sep=",")
    elif type(data_to_be_transformed) is list:
        df_with_pii = pd.DataFrame.from_dict(data_to_be_transformed)
        print(df_with_pii, "<<<")    
    else: 
        logger.error('Unsupported data type.')
        return
    if df_with_pii.empty:
        logger.error('Input data file has no content.')
        return
    for item in pii_fields:
        if item not in df_with_pii.columns:
            logger.error(f'PII field "{item}" not found in file.')
    df_anonymised = pd.DataFrame(columns=df_with_pii.columns)
    for column in df_with_pii.columns:
        if column in pii_fields:
            df_anonymised[column] = ['***'] * len(df_with_pii)
        else:
            df_anonymised[column] = df_with_pii[column]
    if type(data_to_be_transformed) is str:
        csv_buffer = io.BytesIO()
        df_anonymised.to_csv(csv_buffer, index=False)
        output = csv_buffer.getvalue()
    elif type(data_to_be_transformed) is list:
        json_string = df_anonymised.to_json(orient='records')
        output = json.loads(json_string)
    return output


if __name__ == '__main__':
    csv = """student_id,name,course,cohort,graduation_date,email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'"""

    transformation_handler(csv, ["name", "email_address"])
