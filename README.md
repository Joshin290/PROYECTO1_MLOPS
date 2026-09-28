# Proyecto

## Grupo 3

### Integrantes
* **Juan David Clavijo Ortiz**
* **Laura Sofía Rodríguez Pérez**
* **Joshua Alexander Valero Lozano**

Este repositorio contiene la arquitectura completa de MLOps orientada a la automatización, entrenamiento, almacenamiento y despliegue de modelos de Machine Learning para la predicción de cobertura forestal.

## Origen de los Datos y Fuente Externa

Los datos del sistema son obtenidos a través de una API externa expuesta en la máquina virtual del profesor, alojada en la dirección **IP http://10.43.97.110:8080**. Esta API proporciona un conjunto de datos aleatorios que cambian cada 5 minutos, los cuales son recolectados de manera automatizada mediante flujos en **Apache Airflow** para alimentar el entrenamiento de los modelos de inteligencia artificial.

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

| Servicio | Puerto en el Host |
| :--- | :--- |
| **Airflow Webserver** | `8080` | 
| **API de Cobertura Forestal** | `8027` | 
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

## Ejecución de la Canalización (DAG)

Ingresa a la interfaz gráfica de Airflow:

URL: http://localhost:8080

Inicia sesión con las credenciales predeterminadas:

Usuario: airflow

Contraseña: airflow

Dirígete a la pestaña DAGs, busca el flujo correspondiente al proyecto y activa su interruptor para cambiarlo a estado activo.

Haz clic en el botón de reproducción (Trigger DAG) para iniciar una ejecución manual de la ingesta desde la API externa y el procesamiento de los datos.

lujo de Ejecución y Tiempos del Sistema

## secuencia operativa:

1. **Ingesta y Almacenamiento (Airflow):** 
   - Se debe esperar un tiempo estimado de 30 minutos para que se ejecuten y completen de forma periódica los **10 DAGs** programados en Apache Airflow. 
   - Estos flujos se encargan de extraer los lotes de datos desde la API externa del profesor (`http://10.43.97.110:8080`) y almacenarlos y estructurarlos de forma persistente en la base de datos de PostgreSQL.

2. **Entrenamiento (Jupyter Lab):**
   - Una vez finalizada la ingesta masiva en la base de datos, se procede a utilizar el entorno interactivo de Jupyter Lab.
   - Desde el notebook, se consumen los datos preparados previamente en PostgreSQL, se ejecuta el entrenamiento del modelo de Machine Learning, y el artefacto resultante se empaca y almacena directamente en MinIO.

3. **Inferencia en Tiempo Real (FastAPI):**
   - Finalmente, el servicio REST expuesto en FastAPI (`http://localhost:8027/docs`) recupera de manera dinámica el modelo entrenado desde MinIO para procesar las peticiones de predicción en tiempo real.