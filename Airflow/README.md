# Cobertura forestal — grupo 3

Adaptación de tu proyecto Airflow/Penguins para el PDF MLOPS_Proyecto1_2026_2.
API de datos configurada: http://10.43.97.107:8025/data?group_number=3
La dirección privada solo será accesible desde una red que tenga ruta a esa VM.

## Qué copiar

Copia el contenido de esta carpeta Airflow dentro de TU carpeta Airflow existente:

- Reemplaza docker-compose.yaml, Dockerfile, requirements.txt, dags/1_dag_flujo.py y api/main.py.
- Agrega plugins/cobertura_common.py.
- README.md contiene esta guía.
- Conserva tu .env y las carpetas logs, plugins y dags (salvo el archivo reemplazado).
- Haz un respaldo fuera de dags. No dejes otro .py con el DAG antiguo en dags.
- Usa la MISMA carpeta y el mismo nombre de proyecto Compose que antes, para reutilizar los volúmenes.
- No copies la carpeta P2: ya usarás la API externa configurada.

## 1. Detener y respaldar

Pausa penguins_pipeline en Airflow, espera que termine cualquier ejecución activa.
Desde tu carpeta Airflow en Linux, antes de reemplazar archivos:

    cp -a . ../Airflow-respaldo-$(date +%Y%m%d-%H%M%S)
    docker compose down --remove-orphans

Esto detiene contenedores del proyecto, conservando los volúmenes. NO agregues -v.
Ahora copia los archivos del ZIP encima de tu carpeta Airflow.
El DAG anterior puede continuar listado en la interfaz por sus metadatos: déjalo pausado.

## 2. Construir y levantar

Desde esa misma carpeta Airflow:

    mkdir -p dags logs plugins api
    docker compose config --quiet
    docker compose build airflow-webserver cobertura-api
    docker compose up airflow-init
    docker compose up -d
    docker compose ps

Continúa solo si cada comando termina sin error; airflow-init debe salir con código 0.
Si tu .env ya tiene AIRFLOW_UID, consérvalo. En una instalación Linux nueva, añade
AIRFLOW_UID con el valor de `id -u`. No sobrescribas otras variables del .env.
Los archivos copiados y sus directorios deben permitir lectura al usuario del contenedor.

Se mantiene Airflow 2.6.0/Python 3.7 para no migrar simultáneamente tu instalación.
La imagen propia cambia a airflow-cobertura:2.6.0. El Dockerfile fija Airflow y las
dependencias del proyecto y ejecuta pip check. No instala paquetes al iniciar tareas.
No se ha podido construir esta imagen aquí: no hay Docker en el entorno de revisión.

## 3. Comprobar sin consumir un batch

    docker compose exec airflow-worker python -c "import requests; from airflow.providers.postgres.hooks.postgres import PostgresHook; import minio; print(requests.get('http://10.43.97.107:8025/', timeout=15).status_code); print(PostgresHook(postgres_conn_id='postgres_cobertura').get_first('SELECT 1'))"
    docker compose exec airflow-scheduler airflow dags list-import-errors

El primer comando debería imprimir 200 y (1,). El segundo no debe mostrar errores
para cobertura_pipeline. Si aparecen errores de DAG antiguos, revisa los .py adicionales.

## 4. Ejecutar el DAG

Abre http://IP_DE_TU_VM:8080. Usa tus credenciales actuales de Airflow.
Activa cobertura_pipeline. Ejecuta una vez con Trigger DAG si quieres empezar enseguida.
Después deja que el calendario programe las siguientes ejecuciones cada 6 minutos.
No dispares múltiples ejecuciones manuales seguidas: pueden traer el mismo batch.

Tareas:

1. crear_tablas_y_bucket: crea las tablas si faltan y el bucket modelos automáticamente.
2. recolectar_api: realiza UNA petición al endpoint /data y guarda la respuesta original.
3. preprocesar_y_guardar: usa los datos acumulados, limpia, transforma y guarda etapas.
4. entrenar_y_evaluar: entrena RandomForest y calcula métricas sobre el conjunto de prueba.
5. publicar_modelo_minio: publica el modelo con su preprocesador, métricas y manifest latest.json.

La consulta HTTP no tiene reintentos automáticos. Si otra tarea falla, reintenta esa
tarea desde la interfaz: no borres la ingesta para volver a solicitar los datos.
Una ingesta confirmada en PostgreSQL se reutiliza si se limpia la tarea de recolección.
Si la petición falla antes de guardar la respuesta, se reporta como fallo; no hay
transacción distribuida capaz de revertir el contador de la API externa.

Después de guardar 10 números de batch distintos, las siguientes ejecuciones omiten
la recolección y sus tareas posteriores. Pausa el DAG cuando se complete el último
entrenamiento. No se llama al endpoint de reinicio de la API.
Si una primera muestra no alcanza para entrenar, la ingesta queda guardada y otra
ejecución puede completar datos. El fallo se explica en los logs; no se publica un
modelo ficticio ni se da por completado ese entrenamiento.

## 5. Ver los modelos

Consola MinIO: http://IP_DE_TU_VM:9001
Usuario: minioadmin
Contraseña de laboratorio: minioProyecto123
Bucket: modelos (se crea en la primera tarea).

Cada ejecución conserva bajo grupo3/<id-de-ejecucion>/:

- preprocesador.joblib
- candidato.joblib
- modelo.joblib (contiene clasificador y preprocesador)
- metricas.json

El archivo grupo3/latest.json apunta al último modelo publicado correctamente.
No selecciona automáticamente el mejor modelo. La API consulta ese manifiesto,
verifica SHA256 al descargar un modelo nuevo y lo mantiene en memoria hasta el cambio.

## 6. Probar inferencia

Tu API propia: http://IP_DE_TU_VM:8027/docs
No confundir con la API externa de datos en 10.43.97.107:8025.
GET /health comprueba el proceso.
GET /model comprueba MinIO/modelo y devuelve métricas.
POST /predict recibe las 12 variables; NO recibe Cover_Type.

Ejemplo de cuerpo JSON:

```json
{
  "Elevation": 2596,
  "Aspect": 51,
  "Slope": 3,
  "Horizontal_Distance_To_Hydrology": 258,
  "Vertical_Distance_To_Hydrology": 0,
  "Horizontal_Distance_To_Roadways": 510,
  "Hillshade_9am": 221,
  "Hillshade_Noon": 232,
  "Hillshade_3pm": 148,
  "Horizontal_Distance_To_Fire_Points": 6279,
  "Wilderness_Area": "Rawah",
  "Soil_Type": "C7744"
}
```

La respuesta incluye cover_type (1..7), probabilidades por clase entrenada, ejecución
del modelo y categorías desconocidas. Una categoría nueva se codifica como ceros y
se informa en categorias_no_vistas. Antes de publicar el primer modelo, /predict y
/model devuelven 503; /health puede devolver 200.

## Etapas en PostgreSQL

- cobertura_ingestas: respuesta JSON original, grupo, batch, conteos y fecha por ejecución.
- cobertura_raw: observaciones originales válidas, sin repetir entradas idénticas.
- cobertura_processed: instantánea de filas limpias con asignación train/test.
- cobertura_ready: instantánea de variables transformadas y etiqueta, lista para entrenamiento.
- cobertura_modelos: ruta del modelo y métricas por ejecución.

Las entradas se convierten a tipos canónicos antes de calcular su hash. La misma
combinación de entradas queda siempre en el mismo conjunto, aproximadamente 80/20.
La división no es estratificada: por eso se informan las clases presentes en train/test.
Si las mismas entradas llegan con etiquetas contradictorias, se conserva la primera;
la respuesta original completa permite auditar el conflicto.
El escalador y OneHotEncoder se ajustan solo con train. El preprocesamiento completo
viaja dentro del artefacto modelo.joblib que usa la API.
El F1 macro se calcula sobre las clases presentes en y_test/predicciones; la matriz
de confusión siempre tiene orden 1..7. No compares distintos runs como si tuvieran
idéntico test: el test mantiene pertenencia, pero crece al incorporar observaciones.

Consulta de avance:

    docker compose exec postgres-datos psql -U proyecto -d cobertura -c "SELECT batch, count(*) AS ejecuciones, sum(nuevas) AS nuevas FROM cobertura_ingestas WHERE grupo=3 GROUP BY batch ORDER BY batch;"

## Logs

    docker compose logs --tail=100 airflow-worker
    docker compose logs --tail=100 cobertura-api
    docker compose logs --tail=100 minio

Las métricas y conteos se ven también en los logs de cada tarea de Airflow.

## Advertencia concreta sobre el código P2 adjunto

P2/main.py llama get_batch_data(group_number), aunque incrementa un contador batch.
Eso puede devolver muestras del mismo bloque con números de batch diferentes.
Este paquete consume la API de la IP indicada; no altera la API del profesor.
Debes confirmar con quien la desplegó que esté corregida. Contar 10 batch_number
no demuestra por sí solo haber muestreado 10 bloques distintos del dataset.
También observa si nuevas=0 repetidamente o si se repiten demasiadas observaciones.

## Validación y entrega académica

Se revisaron sintaxis compatible con Python 3.7, YAML, servicios/volúmenes referenciados,
y se probó preprocesamiento, serialización e inferencia con datos sintéticos y MinIO
simulado. No se probó acceso a tu IP privada ni ejecución real de Docker, Airflow,
PostgreSQL o MinIO. La prueba local de ML usa las librerías disponibles en el entorno
de revisión; la construcción con las versiones fijadas debe verificarse en tu VM.

Para terminar la entrega: ejecutar los batches reales, comprobar DAG completo en verde,
modelos visibles en MinIO, una predicción real, e incluir estos archivos en el repositorio
público que pide el PDF. No subas .env, logs, caches o datos privados al repositorio.
Las credenciales aquí son valores de laboratorio del Compose que ya estabas usando.

Referencia de instalación Airflow 2.6.0:
https://airflow.apache.org/docs/apache-airflow/2.6.0/installation/installing-from-pypi.html
Cliente MinIO utilizado:
https://pypi.org/project/minio/7.1.17/
