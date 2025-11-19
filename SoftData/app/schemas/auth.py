from pydantic import BaseModel
from app.schemas.usuarios import RetornoUsuario

class ResponseLoggin(BaseModel):
    user: RetornoUsuario
    access_token: str