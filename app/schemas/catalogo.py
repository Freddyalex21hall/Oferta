from typing import Optional
from pydantic import BaseModel, Field

class CatalogoBase(BaseModel):
    nombre_catalogo: str = Field(min_length=3, max_length=100)
    descripcion: Optional[str] = Field(default=None, max_length=500)
    cod_catalogo: Optional[str] = Field(default=None, max_length=50)
    estado: bool = True

class CrearCatalogo(CatalogoBase):
    pass

class RetornoCatalogo(CatalogoBase):
    id_catalogo: int
    
    class Config:
        from_attributes = True

class EditarCatalogo(BaseModel):
    nombre_catalogo: Optional[str] = Field(default=None, min_length=3, max_length=100)
    descripcion: Optional[str] = Field(default=None, max_length=500)
    cod_catalogo: Optional[str] = Field(default=None, max_length=50)
    estado: Optional[bool] = None

