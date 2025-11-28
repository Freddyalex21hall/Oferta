from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.router import usuarios, auth, programas_formacion, programas, historico, cargar_archivos_historico, estado_normas, catalogo, cargar_archivos_registro_calificado, registro_calificado, cargar_archivos


app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
# Incluir en el objeto app los routers
app.include_router(cargar_archivos_historico.router, prefix="/cargar", tags=["Cargar archivos histórico"])
app.include_router(historico.router, prefix="/historico", tags=["servicios histórico"])
app.include_router(cargar_archivos.router,prefix="/cargar_archivos",tags=["Cargar Archivos Estado Normas"]  )
app.include_router(estado_normas.router, prefix="/estado-normas", tags=["Estado de Normas"])
app.include_router(catalogo.router, prefix="/catalogo", tags=["Catalogo"])
app.include_router(programas_formacion.router, prefix="/programas-formacion",tags=["Programas de Formación"])
app.include_router(cargar_archivos_registro_calificado.router, prefix="/cargar_registro_calificado", tags=["Cargar Archivos Registro Calificado"])
app.include_router(registro_calificado.router, prefix="/registro_calificado", tags=["Registro Calificado"])
app.include_router(usuarios.router, prefix="/usuario", tags=["servicios usuarios"])
app.include_router(auth.router, prefix="/access", tags=["servicios de autenticación"])
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
