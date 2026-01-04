from flask import Flask, request, jsonify
from google.cloud import storage
import json
import os
import re

app = Flask(__name__)
storage_client = storage.Client()

# -----------------------
# TEXT CLEANING
# -----------------------
def clean_text(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace('\\"', '"')
        value = value.replace('"', "")
        value = re.sub(r"[\n\r\t]", " ", value)
        value = re.sub(r"[\x00-\x1F]", " ", value)
        value = re.sub(r"\s{2,}", " ", value)
        return value.strip()
    return value


def clean_record(record):
    if not isinstance(record, dict):
        return record

    for k, v in record.items():
        if isinstance(v, str):
            record[k] = clean_text(v)
        elif isinstance(v, list):
            record[k] = [
                clean_record(i) if isinstance(i, dict) else clean_text(i)
                for i in v
            ]
        elif isinstance(v, dict):
            record[k] = clean_record(v)
    return record


# -----------------------
# ROOT 
# -----------------------
@app.route("/", methods=["POST"])
def root():
    return clean_data()


# -----------------------
# CLEAN ENDPOINT
# -----------------------
@app.route("/clean", methods=["POST"])
def clean_data():
    try:
        data = request.get_json(silent=True)
        if not data:
            return {"error": "Invalid JSON body"}, 400

        input_bucket = data["input_bucket"]
        input_prefix = data["input_prefix"]
        output_bucket = data["output_bucket"]
        output_prefix = data["output_prefix"]

        in_bucket = storage_client.bucket(input_bucket)
        out_bucket = storage_client.bucket(output_bucket)

        blobs = list(storage_client.list_blobs(input_bucket, prefix=input_prefix))

        processed = 0

        for blob in blobs:
            if not blob.name.endswith(".json"):
                continue

            try:
                raw = blob.download_as_text(encoding="utf-8").strip()
                if not raw:
                    continue

                # ---- HANDLE JSON ARRAY vs JSONL ----
                if raw.startswith("["):
                    records = json.loads(raw)
                else:
                    records = [
                        json.loads(line)
                        for line in raw.splitlines()
                        if line.strip()
                    ]

                cleaned_records = [clean_record(r) for r in records]

                # output filename
                filename = os.path.basename(blob.name)
                name, ext = os.path.splitext(filename)
                new_name = f"{name}_cleaned{ext}"

                out_path = output_prefix.rstrip("/") + "/" + new_name

                out_blob = out_bucket.blob(out_path)
                out_blob.upload_from_string(
                    json.dumps(cleaned_records, ensure_ascii=False, indent=2),
                    content_type="application/json",
                )

                processed += 1

            except Exception as e:
                print(f"Skip file {blob.name}: {e}")
                continue

        return jsonify({
            "status": "success",
            "files_processed": processed
        })

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"error": str(e)}, 500


# -----------------------
# RUN FOR CLOUD RUN
# -----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
