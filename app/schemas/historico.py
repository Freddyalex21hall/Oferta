from typing import Optional
from pydantic import BaseModel, Field

class HistoricoBase(BaseModel):
    id_grupo: int = Field(gt=0, description="ID del grupo asociado")
    num_aprendices_inscritos: Optional[int] = Field(default=None, ge=0)
    num_aprendices_en_transito: Optional[int] = Field(default=None, ge=0)
    num_aprendices_formacion: Optional[int] = Field(default=None, ge=0)
    num_aprendices_induccion: Optional[int] = Field(default=None, ge=0)
    num_aprendices_condicionados: Optional[int] = Field(default=None, ge=0)
    num_aprendices_aplazados: Optional[int] = Field(default=None, ge=0)
    num_aprendices_retirado_voluntario: Optional[int] = Field(default=None, ge=0)
    num_aprendices_cancelados: Optional[int] = Field(default=None, ge=0)
    num_aprendices_reprobados: Optional[int] = Field(default=None, ge=0)
    num_aprendices_no_aptos: Optional[int] = Field(default=None, ge=0)
    num_aprendices_reingresados: Optional[int] = Field(default=None, ge=0)
    num_aprendices_por_certificar: Optional[int] = Field(default=None, ge=0)
    num_aprendices_certificados: Optional[int] = Field(default=None, ge=0)
    num_aprendices_trasladados: Optional[int] = Field(default=None, ge=0)

class CrearHistorico(HistoricoBase):
    pass

class RetornoHistorico(HistoricoBase):
    id_historico: int

    class Config:
        from_attributes = True

