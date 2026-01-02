FROM python:3.10-slim

WORKDIR /app
COPY main.py .

RUN pip install flask google-cloud-storage

CMD ["python", "main.py"]
