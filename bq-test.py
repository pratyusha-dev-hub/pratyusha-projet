from google.cloud import bigquery
client = bigquery.Client()

print([dataset.dataset_id for dataset in client.list_datasets()])
print("BigQuery connection successful!") 
print("Change from Pratyusha")