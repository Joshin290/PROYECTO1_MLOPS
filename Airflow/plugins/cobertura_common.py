"""Esquema, preprocesamiento y almacenamiento compartidos por DAG y API."""
import hashlib
import io
import json
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder

NUMERICAS = [
    'Elevation', 'Aspect', 'Slope', 'Horizontal_Distance_To_Hydrology',
    'Vertical_Distance_To_Hydrology', 'Horizontal_Distance_To_Roadways',
    'Hillshade_9am', 'Hillshade_Noon', 'Hillshade_3pm',
    'Horizontal_Distance_To_Fire_Points',
]
CATEGORICAS = ['Wilderness_Area', 'Soil_Type']
ENTRADAS = NUMERICAS + CATEGORICAS
COLUMNAS = ENTRADAS + ['Cover_Type']


def limpiar_fila(fila):
    if not isinstance(fila, list) or len(fila) != len(COLUMNAS):
        raise ValueError('Cada fila debe contener 13 valores en el orden documentado.')
    valores = [float(v) for v in fila[:10]]
    if not np.isfinite(valores).all():
        raise ValueError('Valores numéricos no finitos.')
    categorias = [str(v).strip() if v is not None else '' for v in fila[10:12]]
    if any(not v or v.lower() in ('nan', 'none', 'null', 'na') for v in categorias):
        raise ValueError('Categoría vacía.')
    etiqueta = float(fila[12])
    if etiqueta not in range(1, 8):
        raise ValueError('Cover_Type debe ser un entero entre 1 y 7.')
    return valores + categorias + [int(etiqueta)]


def identidad(fila_limpia):
    # Hash de entradas, sin etiqueta: la misma observación no pasa de train a test.
    texto = json.dumps(fila_limpia[:12], ensure_ascii=True, separators=(',', ':'))
    return hashlib.sha256(texto.encode()).hexdigest()


def conjunto(row_id):
    # Separación reproducible ~80/20 que permanece estable al llegar más datos.
    return 'test' if int(row_id[:8], 16) % 100 < 20 else 'train'


def crear_preprocesador():
    return ColumnTransformer([
        ('numericas', MinMaxScaler(), NUMERICAS),
        ('categorias', OneHotEncoder(handle_unknown='ignore'), CATEGORICAS),
    ], sparse_threshold=0)


def dataframe(filas):
    return pd.DataFrame(filas, columns=COLUMNAS)


def cliente_minio():
    from minio import Minio
    return Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=os.environ.get('MINIO_SECURE', 'false').lower() == 'true',
    )


def bucket():
    return os.environ.get('MINIO_BUCKET', 'modelos')


def asegurar_bucket():
    from minio.error import S3Error
    cliente = cliente_minio()
    if not cliente.bucket_exists(bucket()):
        try:
            cliente.make_bucket(bucket())
        except S3Error as error:
            if error.code not in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                raise


def guardar_bytes(nombre, contenido, tipo='application/octet-stream'):
    cliente_minio().put_object(bucket(), nombre, io.BytesIO(contenido),
                               len(contenido), content_type=tipo)


def leer_bytes(nombre):
    respuesta = cliente_minio().get_object(bucket(), nombre)
    try:
        return respuesta.read()
    finally:
        respuesta.close()
        respuesta.release_conn()


def guardar_objeto(nombre, objeto):
    memoria = io.BytesIO()
    joblib.dump(objeto, memoria)
    contenido = memoria.getvalue()
    guardar_bytes(nombre, contenido)
    return hashlib.sha256(contenido).hexdigest()


def leer_objeto(nombre):
    return joblib.load(io.BytesIO(leer_bytes(nombre)))


def prefijo(run_id):
    return 'grupo3/' + hashlib.sha256(run_id.encode()).hexdigest()[:24]
