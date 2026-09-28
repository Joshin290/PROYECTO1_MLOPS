"""Una consulta externa por ejecución; las otras tareas reutilizan PostgreSQL."""
import json
import logging
from datetime import timedelta

import numpy as np
import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
import sklearn

from cobertura_common import (
    ENTRADAS, asegurar_bucket, limpiar_fila, identidad, conjunto,
    crear_preprocesador, dataframe, guardar_objeto, leer_objeto,
    guardar_bytes, prefijo,
)

LOG = logging.getLogger(__name__)


def conexion():
    return PostgresHook(postgres_conn_id='postgres_cobertura').get_conn()


def crear_tablas():
    db = conexion()
    try:
        with db:
            with db.cursor() as c:
                c.execute('''
                CREATE TABLE IF NOT EXISTS cobertura_ingestas (
                    run_id TEXT PRIMARY KEY, grupo INT NOT NULL,
                    batch INT NOT NULL, respuesta JSONB NOT NULL,
                    recibidas INT NOT NULL, nuevas INT NOT NULL,
                    fecha TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS cobertura_raw (
                    grupo INT NOT NULL, row_id TEXT NOT NULL,
                    datos JSONB NOT NULL, primer_batch INT NOT NULL,
                    fecha TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (grupo, row_id)
                );
                CREATE TABLE IF NOT EXISTS cobertura_processed (
                    run_id TEXT NOT NULL, row_id TEXT NOT NULL,
                    datos JSONB NOT NULL, conjunto TEXT NOT NULL,
                    PRIMARY KEY (run_id, row_id)
                );
                CREATE TABLE IF NOT EXISTS cobertura_ready (
                    run_id TEXT NOT NULL, row_id TEXT NOT NULL,
                    features JSONB NOT NULL, etiqueta INT NOT NULL,
                    conjunto TEXT NOT NULL,
                    PRIMARY KEY (run_id, row_id)
                );
                CREATE TABLE IF NOT EXISTS cobertura_modelos (
                    run_id TEXT PRIMARY KEY, objeto TEXT NOT NULL,
                    metricas JSONB NOT NULL,
                    fecha TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                ''')
    finally:
        db.close()
    asegurar_bucket()


def recolectar(**context):
    run_id = context['run_id']
    grupo = int(Variable.get('grupo_proyecto'))
    url = Variable.get('api_datos_url').rstrip('/') + '/data'
    db = conexion()
    try:
        with db:
            with db.cursor() as c:
                c.execute('SELECT batch FROM cobertura_ingestas WHERE run_id=%s', (run_id,))
                if c.fetchone():
                    LOG.info('Esta ejecución ya tiene una respuesta guardada; se reutiliza.')
                    return
                c.execute('SELECT count(DISTINCT batch) FROM cobertura_ingestas WHERE grupo=%s', (grupo,))
                if c.fetchone()[0] >= 10:
                    raise AirflowSkipException('Ya hay 10 batches distintos. Pausa el DAG y revisa resultados.')
                # Sin reintentos HTTP ni bucles: una petición por ejecución.
                respuesta = requests.get(url, params={'group_number': grupo},
                                         timeout=(10, 60), allow_redirects=False)
                if respuesta.status_code != 200:
                    raise RuntimeError('API HTTP {}: {}'.format(respuesta.status_code, respuesta.text[:500]))
                contenido = respuesta.json()
                if contenido.get('group_number') != grupo:
                    raise ValueError('La API devolvió otro grupo.')
                batch = contenido.get('batch_number')
                if not isinstance(batch, int) or isinstance(batch, bool):
                    raise ValueError('batch_number debe ser entero.')
                filas = contenido.get('data')
                if not isinstance(filas, list) or not filas:
                    raise ValueError('La API devolvió data vacío o inválido.')
                nuevas = 0
                invalidas = 0
                for fila in filas:
                    try:
                        limpia = limpiar_fila(fila)
                    except (ValueError, TypeError, OverflowError):
                        invalidas += 1
                        continue
                    row_id = identidad(limpia)
                    c.execute('''INSERT INTO cobertura_raw (grupo, row_id, datos, primer_batch)
                        VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                        (grupo, row_id, Json(fila), batch))
                    nuevas += c.rowcount
                # Conserva también la respuesta original (incluye filas inválidas).
                c.execute('''INSERT INTO cobertura_ingestas
                    (run_id,grupo,batch,respuesta,recibidas,nuevas)
                    VALUES (%s,%s,%s,%s,%s,%s)''',
                    (run_id, grupo, batch, Json(contenido), len(filas), nuevas))
                c.execute('SELECT count(DISTINCT batch) FROM cobertura_ingestas WHERE grupo=%s', (grupo,))
                batches = c.fetchone()[0]
                LOG.info('Grupo=%s batch=%s recibidas=%s nuevas=%s inválidas=%s batches=%s/10',
                         grupo, batch, len(filas), nuevas, invalidas, batches)
    finally:
        db.close()


def preprocesar(**context):
    run_id = context['run_id']
    grupo = int(Variable.get('grupo_proyecto'))
    db = conexion()
    try:
        with db:
            with db.cursor() as c:
                c.execute('SELECT row_id, datos FROM cobertura_raw WHERE grupo=%s ORDER BY row_id', (grupo,))
                registros = c.fetchall()
                if not registros:
                    raise ValueError('No hay observaciones válidas para entrenar.')
                ids = [r[0] for r in registros]
                filas = [limpiar_fila(r[1]) for r in registros]
                datos = dataframe(filas)
                particiones = np.array([conjunto(i) for i in ids])
                train = particiones == 'train'
                test = particiones == 'test'
                if train.sum() < 2 or test.sum() < 1:
                    raise ValueError('Muestra insuficiente para separar entrenamiento/prueba.')
                if datos.loc[train, 'Cover_Type'].nunique() < 2:
                    raise ValueError('Entrenamiento necesita al menos dos clases. Recolecta otro batch.')
                transformador = crear_preprocesador()
                transformador.fit(datos.loc[train, ENTRADAS])
                matriz = transformador.transform(datos[ENTRADAS])
                guardar_objeto(prefijo(run_id) + '/preprocesador.joblib', transformador)
                # Solo reemplaza la instantánea de esta ejecución al reintentar.
                c.execute('DELETE FROM cobertura_processed WHERE run_id=%s', (run_id,))
                c.execute('DELETE FROM cobertura_ready WHERE run_id=%s', (run_id,))
                execute_values(c, '''INSERT INTO cobertura_processed
                    (run_id,row_id,datos,conjunto) VALUES %s''',
                    [(run_id, i, Json(f), str(s)) for i, f, s in zip(ids, filas, particiones)], page_size=500)
                execute_values(c, '''INSERT INTO cobertura_ready
                    (run_id,row_id,features,etiqueta,conjunto) VALUES %s''',
                    [(run_id, i, Json(x.tolist()), int(y), str(s))
                     for i, x, y, s in zip(ids, matriz, datos['Cover_Type'], particiones)], page_size=500)
                LOG.info('Train=%s test=%s variables transformadas=%s', train.sum(), test.sum(), matriz.shape[1])
    finally:
        db.close()


def ejecutar_notebook(**context):
    import hashlib
    from pathlib import Path

    import papermill as pm

    run_id = context["run_id"]
    identificador = hashlib.sha256(
        run_id.encode()
    ).hexdigest()[:24]

    carpeta = Path("/opt/airflow/logs/notebooks")
    carpeta.mkdir(parents=True, exist_ok=True)

    salida = carpeta / (
        f"{identificador}"
        f"_intento_{context['ti'].try_number}.ipynb"
    )

    pm.execute_notebook(
        input_path=(
            "/opt/airflow/notebooks/entrenamiento.ipynb"
        ),
        output_path=str(salida),
        parameters={"run_origen": run_id},
        kernel_name="python3",
        log_output=True,
        cwd="/opt/airflow",
    )

    print(f"Notebook ejecutado y guardado en: {salida}")

with DAG(
    dag_id="cobertura_pipeline",
    description="Grupo 3: API → PostgreSQL → notebook → MinIO",
    start_date=pendulum.datetime(
        2026, 9, 1, tz="America/Bogota"
    ),
    schedule_interval=timedelta(minutes=6),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["cobertura", "grupo3"],
) as dag:
    t1 = PythonOperator(
        task_id="crear_tablas_y_bucket",
        python_callable=crear_tablas,
    )

    t2 = PythonOperator(
        task_id="recolectar_api",
        python_callable=recolectar,
        retries=0,
    )

    t3 = PythonOperator(
        task_id="preprocesar_y_guardar",
        python_callable=preprocesar,
    )

    t4 = PythonOperator(
        task_id="ejecutar_notebook",
        python_callable=ejecutar_notebook,
    )

    t1 >> t2 >> t3 >> t4