FROM apache/airflow:2.6.0-python3.7
COPY requirements.txt /requirements.txt
# Mantener Airflow fijado evita que instalar un proveedor lo actualice.
RUN pip install --no-cache-dir "apache-airflow==2.6.0" -r /requirements.txt \
    && pip check
