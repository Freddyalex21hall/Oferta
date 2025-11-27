# app/schemas/estado_normas.py
from pydantic import BaseModel
from typing import Optional
from datetime import date

class CrearEstadoNorma(BaseModel):
    cod_programa: int
    cod_version: str
    fecha_elaboracion: date
    anio: int
    red_conocimiento: Optional[str] = None
    nombre_ncl: Optional[str] = None
    cod_ncl: Optional[int] = None
    ncl_version: Optional[int] = None
    norma_corte_noviembre: Optional[str] = None
    version: Optional[int] = None
    norma_version: Optional[str] = None
    mesa_sectorial: Optional[str] = None
    tipo_norma: Optional[str] = None
    observacion: Optional[str] = None
    fecha_revision: Optional[date] = None
    tipo_competencia: Optional[str] = None
    vigencia: Optional[str] = None
    fecha_indice: Optional[str] = None

class EditarEstadoNorma(BaseModel):
    cod_version: Optional[str] = None
    fecha_elaboracion: Optional[date] = None
    anio: Optional[int] = None
    red_conocimiento: Optional[str] = None
    nombre_ncl: Optional[str] = None
    cod_ncl: Optional[int] = None
    ncl_version: Optional[int] = None
    norma_corte_noviembre: Optional[str] = None
    version: Optional[int] = None
    norma_version: Optional[str] = None
    mesa_sectorial: Optional[str] = None
    tipo_norma: Optional[str] = None
    observacion: Optional[str] = None
    fecha_revision: Optional[date] = None
    tipo_competencia: Optional[str] = None
    vigencia: Optional[str] = None
    fecha_indice: Optional[str] = None

class RetornoEstadoNorma(BaseModel):
    id_estado_norma: int
    cod_programa: int
    cod_version: str
    fecha_elaboracion: date
    anio: int
    red_conocimiento: Optional[str]
    nombre_ncl: Optional[str]
    cod_ncl: Optional[int]
    ncl_version: Optional[int]
    norma_corte_noviembre: Optional[str]
    version: Optional[int]
    norma_version: Optional[str]
    mesa_sectorial: Optional[str]
    tipo_norma: Optional[str]
    observacion: Optional[str]
    fecha_revision: Optional[date]
    tipo_competencia: Optional[str]
    vigencia: Optional[str]
    fecha_indice: Optional[str]
