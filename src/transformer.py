import pandas as pd
import io
from io import StringIO
import logging


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
def transformation_handler(csv_to_be_transformed, pii_fields):
    logger = setup_logging()
    if len(csv_to_be_transformed) == 0:
        logger.error('csv file is blank')
        return
    df_with_pii = pd.read_csv(StringIO(csv_to_be_transformed), sep=",")
    if len(df_with_pii.index) == 0:
        logger.error('csv file has no content')
        return
    for item in pii_fields:
        if item not in df_with_pii.columns:
            logger.error('pii field "' + item + '" not found in file')
            return
    df_anonymised = pd.DataFrame(columns=df_with_pii.columns)
    for column in df_with_pii.columns:
        if column in pii_fields:
            df_anonymised[column] = ['***'] * len(df_with_pii)
        else:
            df_anonymised[column] = df_with_pii[column]
    csv_buffer = io.BytesIO()
    df_anonymised.to_csv(csv_buffer, index=False)
    csv_byte_stream = csv_buffer.getvalue()
    return csv_byte_stream


if __name__ == '__main__':
    csv = """student_id,name,course,cohort,graduation_date,email_address
1234,'John Smith','Software','August','2024-03-31',\
'j.smith@email.com'"""

    transformation_handler(csv, ["name", "email_address"])
