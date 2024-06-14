# GDPR Obfuscator Module

 <p align="justify">The intended use of the module is to obfuscate all data from columns containing personally identifiable information (PII).</p>

## The Module

<p align="justify">1. The module takes a JSON which contains an AWS S3 bucket location of where the data is stored. The data is extracted from this location. The module can accept CSV, JSON or parquet(bytes) data formats.</p>
<p align="justify">2. The module converts the data to a pandas dataframe and obfuscates all data from any fields which have been defined in the original JSON, transforming all data in that column to "***". The data is then returned in its original file format.</p>

## Example

### Example Input:
```
{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
    "pii_fields": ["name", "email_address"]}
```
### Example Target File:
```
student_id,name,course,cohort,graduation_date,email_address
1234,'John Smith','Software','Jan','2024-03-31','j.smith@email.com'
```

### Example Output:
```
student_id,name,course,cohort,graduation_date,email_address
1234,'***','Software','Jan','2024-03-31','***'
```


## Prerequisites

To use the pipeline, ensure you have met the following requirements:
* You have installed the latest version of Python and set up a virtual environment.
    ```
    python -m venv venv
    source venv/bin/activate
    ```
* You have installed the required packages:
    ```
    make requirements
    ```

### Assumptions

* CSV data is comma separated.
* CSV files have extension .csv.
* JSON files have extension .json.
* Parquet data is in binary format.
* Parquet files have extension .pqt or .parquet.

## Contributor

* Philippa Clarkson [@philupa](https://github.com/philupa)