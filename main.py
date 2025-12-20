from google.cloud import storage
import json

def merge_json(request):
    BUCKET_NAME = "seminar-inferenced-data-bucket"
    OUTPUT_PATH = "merged/all_data.json"

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    merged = []

    for blob in bucket.list_blobs():  
        if blob.name.endswith(".json") and not blob.name.startswith("merged/"):
            try:
                content = blob.download_as_text()
                data = json.loads(content)

                if isinstance(data, list):
                    merged.extend(data)
                else:
                    merged.append(data)

            except Exception as e:
                print(f"Skip {blob.name}: {e}")

    bucket.blob(OUTPUT_PATH).upload_from_string(
        json.dumps(merged, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

    return f"Merged {len(merged)} records"
