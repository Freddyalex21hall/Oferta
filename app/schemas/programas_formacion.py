# app/schemas/programas_formacion.py
from pydantic import BaseModel, Field, AnyUrl
from typing import Optional

class CrearPrograma(BaseModel):
    version: Optional[str] = None
    nombre: str = Field(min_length=3, max_length=255)
    nivel: Optional[str] = None
    meses_duracion: Optional[int] = None
    duracion_programa: Optional[int] = None
    unidad_medida: Optional[str] = None
    estado: Optional[bool] = True
    tipo_programa: Optional[str] = None
    url_pdf: Optional[str] = None
    red_conocimiento: Optional[str] = None
    programa_especial: Optional[int] = None

class EditarPrograma(BaseModel):
    version: Optional[str] = None
    nombre: Optional[str] = None
    nivel: Optional[str] = None
    meses_duracion: Optional[int] = None
    duracion_programa: Optional[int] = None
    unidad_medida: Optional[str] = None
    estado: Optional[bool] = None
    tipo_programa: Optional[str] = None
    url_pdf: Optional[str] = None
    red_conocimiento: Optional[str] = None
    programa_especial: Optional[int] = None

class RetornoPrograma(BaseModel):
    cod_programa: int
    version: Optional[str]
    nombre: str
    nivel: Optional[str]
    meses_duracion: Optional[int]
    duracion_programa: Optional[int]
    unidad_medida: Optional[str]
    estado: Optional[bool]
    tipo_programa: Optional[str]
    url_pdf: Optional[str]
    red_conocimiento: Optional[str]
    programa_especial: Optional[int]
