
FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
# RUN pip install apache-airflow-providers-oracle==3.8.0

USER root
RUN apt-get update && apt-get install -y chromium-driver poppler-utils tesseract-ocr && rm -rf /var/lib/apt/lists/*
