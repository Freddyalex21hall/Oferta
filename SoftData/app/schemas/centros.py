from typing import Optional
from pydantic import BaseModel, Field

class CentroBase(BaseModel):
    nombre_centro: str = Field(min_length=3, max_length=80)
    cod_centro: int
