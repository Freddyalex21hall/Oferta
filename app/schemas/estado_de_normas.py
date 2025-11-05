from typing import Optional
from datetime import date
from pydantic import BaseModel, Field


class EstadoBase(BaseModel):
	cod_version: Optional[str] = Field(default=None, description="Código de versión")
	fecha_elaboracion: Optional[date] = Field(default=None, description="Fecha de elaboración")
	anio: Optional[int] = Field(default=None, ge=0, description="Año asociado")
	red_conocimiento: Optional[str] = Field(default=None)
	nombre_ncl: Optional[str] = Field(default=None)
	cod_ncl: Optional[int] = Field(default=None)
	ncl_version: Optional[int] = Field(default=None)
	norma_corte_noviembre: Optional[str] = Field(default=None)
	verion: Optional[int] = Field(default=None)
	norma_version: Optional[str] = Field(default=None)
	mesa_sectorial: Optional[str] = Field(default=None)
	tipo_norma: Optional[str] = Field(default=None)
	observacion: Optional[str] = Field(default=None)
	fecha_revision: Optional[date] = Field(default=None)
	tipo_competencia: Optional[str] = Field(default=None)
	vigencia: Optional[str] = Field(default=None)
	fecha_indice: Optional[str] = Field(default=None)


class CrearEstado(EstadoBase):
	pass


class RetornoEstado(EstadoBase):
	cod_programa: int

	class Config:
		from_attributes = True


class EditarEstado(BaseModel):
	cod_version: Optional[str] = Field(default=None)
	fecha_elaboracion: Optional[date] = Field(default=None)
	anio: Optional[int] = Field(default=None, ge=0)
	red_conocimiento: Optional[str] = Field(default=None)
	nombre_ncl: Optional[str] = Field(default=None)
	cod_ncl: Optional[int] = Field(default=None)
	ncl_version: Optional[int] = Field(default=None)
	norma_corte_noviembre: Optional[str] = Field(default=None)
	verion: Optional[int] = Field(default=None)
	norma_version: Optional[str] = Field(default=None)
	mesa_sectorial: Optional[str] = Field(default=None)
	tipo_norma: Optional[str] = Field(default=None)
	observacion: Optional[str] = Field(default=None)
	fecha_revision: Optional[date] = Field(default=None)
	tipo_competencia: Optional[str] = Field(default=None)
	vigencia: Optional[str] = Field(default=None)
	fecha_indice: Optional[str] = Field(default=None)
