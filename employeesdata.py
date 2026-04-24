from google.cloud import storage
from google.cloud import bigquery

# -------- CONFIG --------
PROJECT_ID = "project-36c16f64-83f8-4df3-b8b"
BUCKET_NAME = "pratyusha_batch_bucket"   # fixed spelling
DATASET = "emp_data"                     # match your actual dataset
TABLE = "employeestable"
FILE_NAME = "employeesdata.csv"

# -------- STEP 1: CREATE CSV FILE --------
with open(FILE_NAME, "w") as f:
    f.write("emp_id,emp_name,emp_age,emp_sal\n")
    f.write("1,John,30,50000\n")
    f.write("2,Alice,28,60000\n")
    f.write("3,Bob,35,70000\n")

print("CSV file created")

# -------- STEP 2: UPLOAD TO GCS --------
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(FILE_NAME)
blob.upload_from_filename(FILE_NAME)

print(f"Uploaded to: gs://{BUCKET_NAME}/{FILE_NAME}")

# -------- STEP 3: LOAD INTO BIGQUERY --------
bq_client = bigquery.Client()

table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True
)

uri = f"gs://{BUCKET_NAME}/{FILE_NAME}"

load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
load_job.result()

print("Data loaded into BigQuery ✅")