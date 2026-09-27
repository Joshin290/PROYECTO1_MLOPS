FROM apache/airflow:2.6.0-python3.7

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==2.6.0" \
    -r /requirements.txt