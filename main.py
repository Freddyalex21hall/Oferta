from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.crud import cargar_archivos
from app.router import usuarios
from app.router import auth
from app.router import cargar_archivos
from app.router import programas
from app.router import historico
from app.router import cargar_archivos_historico
app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
# Incluir en el objeto app los routers
app.include_router(usuarios.router, prefix="/usuario", tags=["servicios usuarios"])
app.include_router(auth.router, prefix="/access", tags=["servicios de autenticación"])
app.include_router(cargar_archivos_historico.router, prefix="/cargar", tags=["Cargar archivos histórico"])
app.include_router(historico.router, prefix="/historico", tags=["servicios histórico"])
app.include_router(programas.router)
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
                "message": "Funciona!!!!",
                "autor": "Oferta"
            }
