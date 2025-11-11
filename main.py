from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="API Análisis de Datos SENA Risaralda",
    description="Backend para el análisis de datos de formación profesional y normatividad",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir solicitudes desde cualquier origen
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "message": "API operativa",
        "autor": "ADSO 2925888 - Backend SENA Risaralda"
    }
