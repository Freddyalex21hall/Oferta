from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app.schemas.programas_formacion import RetornoPrograma
from app.crud import programas_formacion as crud_programas
from core.database import get_db

router = APIRouter()


@router.get("/listar", response_model=List[RetornoPrograma])
def listar(db: Session = Depends(get_db)):
    return crud_programas.listar_programas(db)


# Obtener por código de programa (endpoint explícito para evitar rutas dinámicas)
@router.get("/obtener-por-cod_programa/{cod_programa}", response_model=RetornoPrograma)
def obtener_por_cod_programa(cod_programa: str, db: Session = Depends(get_db)):
    r = crud_programas.obtener_programa_por_id(db, cod_programa)
    if not r:
        raise HTTPException(status_code=404, detail="Programa no encontrado")
    return r


# Endpoints de consulta por campos y valores únicos
@router.get("/obtener-por-nivel/{nivel}", response_model=List[RetornoPrograma])
def obtener_por_nivel(nivel: str, db: Session = Depends(get_db)):
    r = crud_programas.get_programas_by_nivel(db, nivel)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron programas para este nivel")
    return r


@router.get("/valores-nivel", response_model=List[str])
def valores_nivel(db: Session = Depends(get_db)):
    return crud_programas.get_distinct_niveles(db)


@router.get("/obtener-por-tipo_programa/{tipo_programa}", response_model=List[RetornoPrograma])
def obtener_por_tipo_programa(tipo_programa: str, db: Session = Depends(get_db)):
    r = crud_programas.get_programas_by_tipo_programa(db, tipo_programa)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron programas para este tipo_programa")
    return r


@router.get("/valores-tipo_programa", response_model=List[str])
def valores_tipo_programa(db: Session = Depends(get_db)):
    return crud_programas.get_distinct_tipo_programa(db)


@router.get("/obtener-por-red_conocimiento/{red}", response_model=List[RetornoPrograma])
def obtener_por_red_conocimiento(red: str, db: Session = Depends(get_db)):
    r = crud_programas.get_programas_by_red_conocimiento(db, red)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron programas para esta red_conocimiento")
    return r


@router.get("/valores-red_conocimiento", response_model=List[str])
def valores_red_conocimiento(db: Session = Depends(get_db)):
    return crud_programas.get_distinct_red_conocimiento(db)


@router.get("/obtener-por-estado/{estado}", response_model=List[RetornoPrograma])
def obtener_por_estado(estado: bool, db: Session = Depends(get_db)):
    r = crud_programas.get_programas_by_estado(db, estado)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron programas para este estado")
    return r


@router.get("/valores-estado", response_model=List[bool])
def valores_estado(db: Session = Depends(get_db)):
    return crud_programas.get_distinct_estados(db)
