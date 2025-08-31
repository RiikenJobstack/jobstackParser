import os
from azure.data.tables import TableClient, UpdateMode

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
table_name = os.getenv("TABLE_NAME", "JobStatus")
PK = "jobs"

table_client = TableClient.from_connection_string(conn_str, table_name=table_name)
try:
    table_client.create_table()
except:
    pass

def save_job_queued(job_id, user_id):
    entity = {
        "PartitionKey": PK,
        "RowKey": job_id,
        "userId": user_id,
        "status": "queued"
    }
    table_client.upsert_entity(entity, mode=UpdateMode.MERGE)
    return entity

def set_job_processing(job_id):
    table_client.upsert_entity({
        "PartitionKey": PK,
        "RowKey": job_id,
        "status": "processing"
    }, mode=UpdateMode.MERGE)

def set_job_done(job_id, result_blob_path):
    table_client.upsert_entity({
        "PartitionKey": PK,
        "RowKey": job_id,
        "status": "done",
        "resultBlobPath": result_blob_path
    }, mode=UpdateMode.MERGE)

def set_job_error(job_id, error_msg):
    table_client.upsert_entity({
        "PartitionKey": PK,
        "RowKey": job_id,
        "status": "error",
        "error": error_msg[:32000]
    }, mode=UpdateMode.MERGE)

def get_job(job_id):
    try:
        return table_client.get_entity(PK, job_id)
    except:
        return None
