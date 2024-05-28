import pandas as pd
import io
from io import BytesIO, StringIO


#TRANSFORMATION FUNCTIONS
def transformation_handler(csv_to_be_transformed, pii_fields):
    df_with_pii = pd.read_csv(StringIO(csv_to_be_transformed), sep=",")
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
    csv = """student_id,name,course,cohort,graduation_date,email_address\n
            1234,'John Smith','Software','August','2024-03-31','j.smith@email.com'"""
    
    transformation_handler(csv, ["name", "email_address"])