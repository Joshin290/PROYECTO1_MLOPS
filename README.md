# Proyecto

## Servicios Implementados

### Apache Airflow:

Su función es automatizar y orquestar los flujos de trabajo. Opera programando de forma periódica la extracción por lotes (batches) desde la API externa.

### PostgreSQL:

Su función en el proyecto es servir como el motor de base de datos relacional y dividirse en dos propósitos críticos: por un lado, almacena los metadatos operativos necesarios para el funcionamiento interno de Apache Airflow; y por otro, aloja la base de datos principal, la cual está encargada de almacenar y estructurar de forma persistente los datos extraídos de cada batch en cada una de las ejecuciones individuales de los DAGs.

### Jupyter Lab: 

Su función es proporcionar un entorno interactivo de desarrollo y experimentación analítica conectado a los volúmenes del sistema. En este proyecto, opera ejecutando un notebook que recibe los datos preparados desde PostgreSQL para entrenar los modelos de Machine Learning, el cual va a dejar almacenado en MinIO

### MinIO: 

Se utilizará como almacenamiento de objetos para los modelos entrenados. Opera como un repositorio centralizado

### Inference API (FastAPI): 

Su función es servir como el servicio REST para consumir de manera dinámica los modelos almacenados en MinIO. Funciona cargando en caliente los artefactos desde el almacenamiento de objetos y procesando peticiones HTTP en tiempo real para devolver las predicciones correspondientes.


## Puertos

A continuación, se describen los puertos usados en este proyecto: 

| Servicio | Puerto en el Host | Descripción |
| :--- | :--- | :--- |
| **Airflow Webserver** | `8080` | 
| **API de CObertura Forestal** | `8027` | 
| **Jupyter Lab** | `8020` | 
| **MinIO Console** | `9001` | 
| **MinIO API** | `9000` | 
| **PostgreSQL (Datos)** | `8030` | 
| **Flower** | `5555` |

## Credenciales

* **Apache Airflow (Interfaz Web):**
  * **Usuario:** `airflow`
  * **Contraseña:** `airflow`

* **PostgreSQL (Metadatos de Airflow):**
  * **Usuario:** `airflow`
  * **Contraseña:** `airflow`

* **PostgreSQL (Base de Datos - Cobertura):**
  * **Usuario:** `proyecto`
  * **Contraseña:** `proyecto123`

* **MinIO (Almacenamiento de Objetos):**
  * **Access Key:** `minioadmin`
  * **Secret Key:** `minioProyecto123`


## Construir y levantar los servicios

Ejecuta el siguiente comando para construir las imágenes personalizadas (como Airflow, Jupyter y la API) y poner en marcha todos los contenedores en segundo plano (detached mode):

**docker compose up --build -d**
