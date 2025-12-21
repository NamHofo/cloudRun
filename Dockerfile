FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .

CMD ["gunicorn", "-b", ":8080", "--timeout", "300", "main:app"]

