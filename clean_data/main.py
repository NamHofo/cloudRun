import json
import re
import os
from flask import Flask, request, jsonify
from google.cloud import storage

app = Flask(__name__)
client = storage.Client()

def clean_text(text):
    if not text:
        return None
    text = re.sub(r'\\"', '"', text)
    text = text.replace('"', '')
    text = re.sub(r'[\n\r\t]', ' ', text)
    text = re.sub(r'[\x00-\x1F]', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip() or None

def normalize_content(content):
    if isinstance(content, list):
        return clean_text(" ".join(content))
    if isinstance(content, dict):
        return clean_text(content.get("string"))
    return clean_text(content)

def clean_record(record):
    record["title"] = clean_text(record.get("title"))
    record["sapo"] = clean_text(record.get("sapo"))
    record["author"] = clean_text(record.get("author"))
    record["content"] = normalize_content(record.get("content"))
    return record

@app.route("/clean", methods=["POST"])
def clean_data():
    body = request.json

    input_bucket = body["input_bucket"]
    input_prefix = body["input_prefix"]
    output_bucket = body["output_bucket"]
    output_prefix = body["output_prefix"]

    src_bucket = client.bucket(input_bucket)
    dst_bucket = client.bucket(output_bucket)

    blobs = src_bucket.list_blobs(prefix=input_prefix)

    processed = 0

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        data = blob.download_as_text().splitlines()
        cleaned = []

        for line in data:
            record = json.loads(line)
            cleaned.append(json.dumps(clean_record(record), ensure_ascii=False))

        filename = os.path.basename(blob.name).replace(".json", "_cleaned.json")
        dst_blob = dst_bucket.blob(f"{output_prefix}/{filename}")
        dst_blob.upload_from_string("\n".join(cleaned), content_type="application/json")

        processed += 1

    return jsonify({"status": "success", "files_processed": processed})

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
