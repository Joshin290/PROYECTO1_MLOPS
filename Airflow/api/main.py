"""Inferencia forestal: carga el último modelo completo publicado en MinIO."""
import hashlib
import io
import json
import logging
import threading

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator

from cobertura_common import ENTRADAS, CATEGORICAS, leer_bytes

app = FastAPI(title='API de Cobertura Forestal - Grupo 3', version='1.0')
LOG = logging.getLogger(__name__)
_cache = {'version': None, 'paquete': None}
_lock = threading.Lock()


class Entrada(BaseModel):
    Elevation: float = Field(..., example=2596)
    Aspect: float = Field(..., ge=0, le=360, example=51)
    Slope: float = Field(..., ge=0, le=90, example=3)
    Horizontal_Distance_To_Hydrology: float = Field(..., ge=0, example=258)
    Vertical_Distance_To_Hydrology: float = Field(..., example=0)
    Horizontal_Distance_To_Roadways: float = Field(..., ge=0, example=510)
    Hillshade_9am: float = Field(..., ge=0, le=255, example=221)
    Hillshade_Noon: float = Field(..., ge=0, le=255, example=232)
    Hillshade_3pm: float = Field(..., ge=0, le=255, example=148)
    Horizontal_Distance_To_Fire_Points: float = Field(..., ge=0, example=6279)
    Wilderness_Area: str = Field(..., min_length=1, example='Rawah')
    Soil_Type: str = Field(..., min_length=1, example='C7744')

    @validator('Wilderness_Area', 'Soil_Type')
    def categoria_valida(cls, valor):
        valor = valor.strip()
        if not valor or valor.lower() in ('none', 'null', 'nan', 'na'):
            raise ValueError('La categoría no puede estar vacía.')
        return valor

    class Config:
        allow_inf_nan = False
        extra = 'forbid'


def cargar_modelo():
    try:
        with _lock:
            manifiesto = json.loads(leer_bytes('grupo3/latest.json'))
            version = manifiesto['sha256']
            if _cache['version'] != version:
                contenido = leer_bytes(manifiesto['objeto'])
                if hashlib.sha256(contenido).hexdigest() != version:
                    raise ValueError('El checksum del modelo no coincide.')
                paquete = joblib.load(io.BytesIO(contenido))
                if paquete['columnas'] != ENTRADAS:
                    raise ValueError('El esquema del modelo no coincide con la API.')
                _cache.update(version=version, paquete=paquete)
                LOG.info('Modelo cargado: %s', manifiesto['objeto'])
            return _cache['paquete']
    except Exception:
        LOG.exception('No se pudo cargar el modelo desde MinIO')
        raise HTTPException(status_code=503, detail=(
            'Modelo no disponible. Verifica MinIO y ejecuta el DAG completo.'))


@app.get('/')
def inicio():
    return {'servicio': 'Cobertura forestal', 'grupo': 3, 'documentacion': '/docs'}


@app.get('/health')
def salud():
    # Comprueba el proceso de la API; /model comprueba también modelo y MinIO.
    return {'status': 'ok'}


@app.get('/model')
def info_modelo():
    paquete = cargar_modelo()
    return {'run_id': paquete['run_id'], 'metricas': paquete['metricas'],
            'columnas': paquete['columnas']}


@app.post('/predict')
def predecir(entrada: Entrada):
    paquete = cargar_modelo()
    datos = pd.DataFrame([entrada.dict()], columns=ENTRADAS)
    transformador = paquete['preprocesador']
    X = transformador.transform(datos)
    modelo = paquete['modelo']
    probabilidades = modelo.predict_proba(X)[0]
    conocidas = transformador.named_transformers_['categorias'].categories_
    desconocidas = [col for col, cats in zip(CATEGORICAS, conocidas)
                   if datos.iloc[0][col] not in cats]
    return {
        'cover_type': int(modelo.predict(X)[0]),
        'probabilidades': {str(int(c)): float(p)
                           for c, p in zip(modelo.classes_, probabilidades)},
        'run_id_modelo': paquete['run_id'],
        'categorias_no_vistas': desconocidas,
    }
