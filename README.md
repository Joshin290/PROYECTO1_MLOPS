# Proyecto

Puerto 8000 - API de estracción de datos
Puerto 8002 - Minio (Endpoint)
Puerto 8001 - Minio (navegador)
Puerto 8080 - Airflow
Puerto 8025 - Jupyter
Puerto 8010 - API de inferencia

Nota: Es necesario crearse licencia de minio al siguiente link: https://www.min.io/pricing  (free)


docker rm -f $(docker ps -a -q)  - Detener y Eliminar todos los contenedores
10.43.97.109