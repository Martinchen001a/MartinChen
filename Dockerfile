FROM apache/airflow:2.10.4-python3.10

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .