from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.router import historico
from app.router import estado_de_normas
# from app.router import centros

app = FastAPI()

# Incluir en el objeto app los routers
app.include_router(historico.router, prefix="/historico", tags=["servicios historico"])
app.include_router(estado_de_normas.router, prefix="/estado_de_normas", tags=["estado_de_normas"])
# app.include_router(centros.router, prefix="/centro", tags=["servicios de Centros de Formación"])

# Configuración de CORS para permitir todas las solicitudes desde cualquier origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir solicitudes desde cualquier origen
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],  # Permitir estos métodos HTTP
    allow_headers=["*"],  # Permitir cualquier encabezado en las solicitudes
)

@app.get("/")
def read_root():
    return {
                "message": "ok",
                "autor": "ADSO 2925888"
            }
