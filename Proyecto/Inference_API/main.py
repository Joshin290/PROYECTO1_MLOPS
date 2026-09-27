from enum import IntEnum
from pathlib import Path
import shutil
import joblib
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# ------------------------------------------------------------------------------
# Configuración de Rutas, Constantes y MinIO
# ------------------------------------------------------------------------------
MODELS_DIR = Path("./modelos")
MODELS_DIR.mkdir(parents=True, exist_ok=True)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = "ml-models"
PREFIX = "models/"

# Cliente S3 para conectarse a MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def download_model_from_minio(filename: str) -> bool:
    """Descarga un modelo desde MinIO a la carpeta local."""
    local_path = MODELS_DIR / filename
    object_name = f"{PREFIX}{filename}"
    try:
        s3_client.download_file(BUCKET_NAME, object_name, str(local_path))
        return True
    except ClientError as e:
        print(f"Error al descargar {filename} de MinIO: {e}")
        return False

# ------------------------------------------------------------------------------
# Definición de Menú/Opciones con IntEnum
# ------------------------------------------------------------------------------
class ModelOption(IntEnum):
    RANDOM_FOREST = 1
    SVC = 2
    KNN = 3

MODEL_MAP = {
    ModelOption.RANDOM_FOREST: "random_forest",
    ModelOption.SVC: "svc",
    ModelOption.KNN: "knn",
}

# ------------------------------------------------------------------------------
# Esquema Pydantic para el Dataset "Tipo de Cubierta Forestal"
# ------------------------------------------------------------------------------
class ModelSelection(BaseModel):
    model_id: ModelOption = Field(
        ...,
        title="ID del Modelo",
        description="Número del modelo a activar:\n- 1: Random Forest\n- 2: Support Vector Machine (SVC)\n- 3: K-Nearest Neighbors (KNN)",
        example=1,
    )

class ForestCoverPredictionInput(BaseModel):
    Elevation: float = Field(..., description="Elevation in meters", example=2596.0)
    Aspect: float = Field(..., description="Aspect in degrees azimuth", example=51.0)
    Slope: float = Field(..., description="Slope in degrees", example=3.0)
    Horizontal_Distance_To_Hydrology: float = Field(..., description="Horz Dist to nearest surface water features", example=258.0)
    Vertical_Distance_To_Hydrology: float = Field(..., description="Vert Dist to nearest surface water features", example=0.0)
    Horizontal_Distance_To_Roadways: float = Field(..., description="Horz Dist to nearest roadway", example=510.0)
    Hillshade_9am: float = Field(..., description="Hillshade index at 9am, summer solstice (0 to 255)", example=221.0)
    Hillshade_Noon: float = Field(..., description="Hillshade index at noon, summer solstice (0 to 255)", example=232.0)
    Hillshade_3pm: float = Field(..., description="Hillshade index at 3pm, summer solstice (0 to 255)", example=148.0)
    Horizontal_Distance_To_Fire_Points: float = Field(..., description="Horz Dist to nearest wildfire ignition points", example=6279.0)
    Wilderness_Area: list[int] = Field(
        ..., 
        description="Wilderness area designation (4 binary columns)", 
        example=[1, 0, 0, 0],
        min_items=4, 
        max_items=4
    )
    Soil_Type: list[int] = Field(
        ..., 
        description="Soil Type designation (40 binary columns)", 
        example=[0]*40,
        min_items=40,
        max_items=40
    )


# ------------------------------------------------------------------------------
# Aplicación FastAPI
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Taller 2 MLOps - Forest Cover",
    description="API para la inferencia de tipos de cubierta forestal y cambio dinámico de modelos conectados a MinIO.",
    version="1.0.0",
)

@app.on_event("startup")
def startup_event():
    print("Inicializando API... Descargando modelo activo desde MinIO.")
    download_model_from_minio("modelo_activo.pkl")

@app.get("/model", tags=["Modelo"])
def get_active_model():
    active_file = MODELS_DIR / "modelo_activo.pkl"

    if not active_file.exists():
        raise HTTPException(
            status_code=404,
            detail="No se encontró ningún modelo activo localmente. Asegúrate de que MinIO tenga el archivo 'modelo_activo.pkl'.",
        )

    try:
        model = joblib.load(active_file)
        return {
            "active_model_file": active_file.name,
            "algorithm_class": type(model).__name__,
            "details": str(model),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al leer el modelo activo: {str(e)}"
        )

@app.post("/select-model", tags=["Modelo"])
def select_model(selection: ModelSelection):
    selected_enum = selection.model_id

    if selected_enum not in MODEL_MAP:
        raise HTTPException(
            status_code=400,
            detail="Opción inválida. Opciones disponibles: 1 (Random Forest), 2 (SVC), 3 (KNN).",
        )

    selected_name = MODEL_MAP[selected_enum]
    filename = f"{selected_name}.pkl"
    source_file = MODELS_DIR / filename
    active_file = MODELS_DIR / "modelo_activo.pkl"

    if not download_model_from_minio(filename):
        raise HTTPException(
            status_code=404,
            detail=f"El archivo '{filename}' no se pudo descargar de MinIO en el bucket '{BUCKET_NAME}'.",
        )

    try:
        shutil.copy(source_file, active_file)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al copiar el modelo seleccionado a activo: {str(e)}",
        )
        
    try:
        s3_client.upload_file(str(active_file), BUCKET_NAME, f"{PREFIX}modelo_activo.pkl")
    except Exception as e:
        print(f"Advertencia: No se pudo subir el nuevo modelo activo a MinIO: {e}")

    return {
        "message": "Modelo descargado de MinIO y actualizado exitosamente",
        "selected_id": int(selected_enum),
        "active_model": selected_name,
    }

@app.post("/predict", tags=["Inferencia"])
def predict(features: ForestCoverPredictionInput):
    """Realiza una predicción utilizando el modelo activo actual para el tipo de cubierta forestal."""
    active_file = MODELS_DIR / "modelo_activo.pkl"

    if not active_file.exists():
        raise HTTPException(
            status_code=404, detail="No hay ningún modelo activo cargado localmente."
        )

    try:
        model = joblib.load(active_file)
        
        # Ensamblar las 10 características cuantitativas iniciales
        feature_vector = [
            features.Elevation,
            features.Aspect,
            features.Slope,
            features.Horizontal_Distance_To_Hydrology,
            features.Vertical_Distance_To_Hydrology,
            features.Horizontal_Distance_To_Roadways,
            features.Hillshade_9am,
            features.Hillshade_Noon,
            features.Hillshade_3pm,
            features.Horizontal_Distance_To_Fire_Points,
        ]
        
        # Extender la lista con las 4 columnas binarias de Wilderness_Area
        feature_vector.extend(features.Wilderness_Area)
        
        # Extender la lista con las 40 columnas binarias de Soil_Type
        feature_vector.extend(features.Soil_Type)
        
        # Formato final esperado por Scikit-Learn (Array 2D de 1 fila x 54 columnas)
        input_data = [feature_vector]

        prediction = model.predict(input_data)[0]

        return {
            "prediction_code": int(prediction),
            "predicted_class": f"Cover_Type_{int(prediction)}",
            "model_used": type(model).__name__,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error durante la inferencia: {str(e)}"
        )