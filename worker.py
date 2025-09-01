import json, time
from queue_service import dequeue_batch, delete_message
from table_service import set_job_processing, set_job_done, set_job_error
from blob_service import get_json, put_json, input_container, output_container
from utils import parse_resume_cached
import base64

def process_job(job):
    job_data = json.loads(job.content)
    job_id = job_data["jobId"]
    user_id = job_data.get("userId")
    blob_name = job_data.get("filename")

    try:
        print(f"Processing job {job_id} for user {user_id}")
        set_job_processing(job_id)

        # --- Load raw resume from blob ---
        input_data = get_json(input_container, blob_name)
       
        filename = input_data["filename"]
        if filename == "resume.txt":
            raw_data = input_data["data"]
            parsed_result = parse_resume_cached(filename, raw_data.encode("utf-8"))
        else:
            # ✅ decode back to bytes
            file_bytes = base64.b64decode(input_data["data_base64"])
            parsed_result = parse_resume_cached(filename, file_bytes)

        # --- Save parsed result to blob ---
        result_blob = f"{job_id}.json"
        blob_path = put_json(output_container, result_blob, parsed_result)

        # --- Mark job done ---
        set_job_done(job_id, blob_path)
        print(f"✅ Job {job_id} done -> {blob_path}")

    except Exception as e:
        set_job_error(job_id, str(e))
        print(f"❌ Job {job_id} failed: {e}")

def run_worker():
    while True:
        messages = dequeue_batch()
        for m in messages:
            process_job(m)
            delete_message(m)
        time.sleep(5)

if __name__ == "__main__":
    run_worker()
