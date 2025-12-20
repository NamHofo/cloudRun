from google.cloud import storage
import json

def merge_json(request):
    client = storage.Client()
    bucket = client.bucket("YOUR_BUCKET_NAME")

    merged = []

    for blob in bucket.list_blobs(prefix="data/"):
        if blob.name.endswith(".json"):
            data = json.loads(blob.download_as_text())
            merged.extend(data if isinstance(data, list) else [data])

    bucket.blob("data/merged.json").upload_from_string(
        json.dumps(merged),
        content_type="application/json"
    )

    return "DONE"
