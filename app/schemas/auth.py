from pydantic import BaseModel

from app.schemas.usuarios import RetornoUsuario, CrearUsuario;

class ResponseLoggin(BaseModel):
    user: RetornoUsuario
    access_token: str
