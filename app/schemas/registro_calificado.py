from typing import Optional
from pydantic import BaseModel, Field


class RegistroCalificadoBase(BaseModel):
    tipo_tramite: Optional[str] = Field(default=None, max_length=50)
    fecha_radicado: Optional[str] = None
    numero_resolucion: Optional[int] = Field(default=None, ge=0)
    fecha_resolucion: Optional[str] = None
    fecha_vencimiento: Optional[str] = None
    vigencia: Optional[str] = Field(default=None, max_length=25)
    modalidad: Optional[str] = Field(default=None, max_length=25)
    clasificacion: Optional[str] = Field(default=None, max_length=15)
    estado_catalogo: Optional[str] = Field(default=None, max_length=50)


class CrearRegistroCalificado(RegistroCalificadoBase):
    cod_programa: str = Field(min_length=1, max_length=16)



class RetornoRegistroCalificado(RegistroCalificadoBase):
    cod_programa: int

    class Config:
        from_attributes = True


class EditarRegistroCalificado(BaseModel):
    tipo_tramite: Optional[str] = Field(default=None, max_length=50)
    fecha_radicado: Optional[str] = None
    numero_resolucion: Optional[int] = Field(default=None, ge=0)
    fecha_resolucion: Optional[str] = None
    fecha_vencimiento: Optional[str] = None
    vigencia: Optional[str] = Field(default=None, max_length=25)
    modalidad: Optional[str] = Field(default=None, max_length=25)
    clasificacion: Optional[str] = Field(default=None, max_length=15)
    estado_catalogo: Optional[str] = Field(default=None, max_length=50)
