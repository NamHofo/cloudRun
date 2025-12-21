from google.cloud import storage
import json
import tempfile
import os

BUCKET_NAME = "seminar-inferenced-data-bucket"
OUTPUT_PATH = "merged/all_data.json"

def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # File tạm để stream data
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as tmp:
        tmp_path = tmp.name
        first = True
        tmp.write("[")

        for blob in bucket.list_blobs():
            if not blob.name.endswith(".json"):
                continue
            if blob.name.startswith("merged/"):
                continue

            try:
                content = blob.download_as_text()
                data = json.loads(content)

                records = data if isinstance(data, list) else [data]

                for record in records:
                    if not first:
                        tmp.write(",\n")
                    json.dump(record, tmp, ensure_ascii=False)
                    first = False

            except Exception as e:
                print(f"Skip {blob.name}: {e}")

        tmp.write("]")

    # Upload file đã merge
    bucket.blob(OUTPUT_PATH).upload_from_filename(
        tmp_path,
        content_type="application/json"
    )

    os.remove(tmp_path)
    print("✅ MERGE DONE")

if __name__ == "__main__":
    main()
