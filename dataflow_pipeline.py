import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# -------- CONFIG --------
PROJECT_ID = "project-36c16f64-83f8-4df3-b8b"
BUCKET_NAME = "pratyusha_batch_bucket"
DATASET = "emp_data"
TABLE = "employeestable"

INPUT_FILE = f"gs://{BUCKET_NAME}/employeesdata.csv"

# -------- TRANSFORM FUNCTION (Fix 2: safe parsing) --------
class TransformData(beam.DoFn):
    def process(self, element):
        fields = element.split(",")

        try:
            yield {
                "emp_id": int(fields[0]),
                "emp_name": fields[1],
                "emp_age": int(fields[2]),
                "emp_sal": float(fields[3]) * 1.1   # 10% hike
            }
        except:
            pass   # skip bad rows

# -------- PIPELINE OPTIONS --------
options = PipelineOptions(
    runner='DataflowRunner',
    project=PROJECT_ID,
    temp_location=f'gs://{BUCKET_NAME}/temp',
    staging_location=f'gs://{BUCKET_NAME}/staging',
    region='us-central1'
)

# -------- PIPELINE --------
with beam.Pipeline(options=options) as p:

    (
        p
        | "Read from GCS" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
        | "Transform Data" >> beam.ParDo(TransformData())
        | "Write to BQ" >> beam.io.WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
            schema="emp_id:INTEGER, emp_name:STRING, emp_age:INTEGER, emp_sal:FLOAT",
            
            # Fix 3: overwrite or append safely
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            # (use WRITE_TRUNCATE if schema mismatch)

            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,

            # Fix 1: REQUIRED for Dataflow BigQuery loads
            custom_gcs_temp_location=f"gs://{BUCKET_NAME}/temp"
        )
    )