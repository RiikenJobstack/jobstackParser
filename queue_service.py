import os
import json
from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceExistsError
from dotenv import load_dotenv

# Load .env file
load_dotenv()

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
queue_name = os.getenv("QUEUE_NAME")

# Initialize queue client
queue_client = QueueClient.from_connection_string(conn_str, queue_name)

def ensure_queue():
    """Ensure queue exists (create only if not present)."""
    try:
        queue_client.create_queue()
        print(f"Queue '{queue_name}' created.")
    except ResourceExistsError:
        # Queue already exists — safe to ignore
        pass

def enqueue_job(job_id, user_id, filename=None):
    """Send a job message to the queue."""
    ensure_queue()
    msg = json.dumps({
        "jobId": job_id,
        "userId": user_id,
        "filename": filename
    })
    queue_client.send_message(msg)

def dequeue_batch(max_messages=16, visibility_timeout=30):
    """Receive a batch of messages from the queue."""
    ensure_queue()
    messages = queue_client.receive_messages(
        messages_per_page=max_messages,
        visibility_timeout=visibility_timeout
    )
    return messages

def delete_message(message):
    """Delete a processed message from the queue."""
    queue_client.delete_message(message.id, message.pop_receipt)
