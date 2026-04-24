from google.cloud import bigquery

# Create client (uses ADC automatically)
client = bigquery.Client()

# Project + Dataset + Table
table_id = "project-36c16f64-83f8-4df3-b8b.employee_data.employees"

# Load CSV file
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # skip header
    autodetect=True
)

with open("C:/Users/SANJEEV/Downloads/employees.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Wait for job to complete

print("✅ Data uploaded successfully!")