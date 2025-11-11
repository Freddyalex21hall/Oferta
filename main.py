from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.router import estado_normas, programas_formacion

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

app.include_router(
    estado_normas.router,
    prefix="/estado-normas",
    tags=["Estado de Normas"]
)

app.include_router(
    programas_formacion.router,
    prefix="/programas-formacion",
    tags=["Programas de Formación"]
)

@app.get("/")
def read_root():
    return {
        "message": "API operativa",
        "autor": "ADSO 2925888 - Backend SENA Risaralda"
    }
