import os, json
from azure.storage.blob import BlobServiceClient

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
input_container = os.getenv("INPUT_CONTAINER", "analyze-inputs")
output_container = os.getenv("OUTPUT_CONTAINER", "analyze-results")

blob_service = BlobServiceClient.from_connection_string(conn_str)

def ensure_containers():
    for container in [input_container, output_container]:
        try:
            blob_service.create_container(container)
        except:
            pass

def put_json(container, name, obj):
    ensure_containers()
    container_client = blob_service.get_container_client(container)
    blob_client = container_client.get_blob_client(name)
    data = json.dumps(obj).encode("utf-8")
    blob_client.upload_blob(data, overwrite=True)
    return f"{container}/{name}"

def get_json(container, name):
    container_client = blob_service.get_container_client(container)
    blob_client = container_client.get_blob_client(name)
    blob_data = blob_client.download_blob().readall()
    return json.loads(blob_data.decode("utf-8"))
