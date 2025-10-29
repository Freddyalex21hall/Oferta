
from typing import Optional
from pydantic import BaseModel, EmailStr, Field


class UsuarioBase(BaseModel):
    nombre_completo: str = Field(min_length=3, max_length=80)
    id_rol: int
    correo: EmailStr
    num_documento: str = Field(min_length=8, max_length=12)

class CrearUsuario(UsuarioBase):
    contra_encript: str = Field(min_length=8)
    estado: bool = True

class RetornoUsuario(UsuarioBase):
    id_usuario: int
    estado: bool
    nombre_rol: str

class EditarUsuario(BaseModel):
    nombre_completo: Optional[str] = Field(default=None, min_length=3, max_length=80)
    correo: Optional[EmailStr] = Field(default=None, min_length=6, max_length=100)
    num_documento: Optional[str] = Field(default=None, min_length=8, max_length=12)
    estado: Optional[bool] = None

class EditarPass(BaseModel):
    id_usuario: int
    contra_anterior: str = Field(min_length=8)
    contra_nueva: str = Field(min_length=8)