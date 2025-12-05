from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from core.database import get_db
from app.schemas.estado_normas import RetornoEstadoNorma
from app.crud import estado_normas as crud_estado

router = APIRouter()


# Listar todos
@router.get("/listar", response_model=List[RetornoEstadoNorma])
def listar(db: Session = Depends(get_db)):
    return crud_estado.listar_estado_normas(db)


# Obtener por ID
@router.get("/obtener/{id}", response_model=RetornoEstadoNorma)
def obtener(id: int, db: Session = Depends(get_db)):
    norma = crud_estado.obtener_estado_norma(db, id)
    if not norma:
        raise HTTPException(status_code=404, detail="Estado de norma no encontrado")
    return norma


# Endpoints de consulta por campos y para valores únicos (selects)
@router.get("/obtener-por-cod_programa/{cod_programa}", response_model=List[RetornoEstadoNorma])
def obtener_por_cod_programa(cod_programa: int, db: Session = Depends(get_db)):
    r = crud_estado.get_estado_by_cod_programa(db, cod_programa)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para este cod_programa")
    return r


@router.get("/valores-anio", response_model=List[int])
def valores_anio(db: Session = Depends(get_db)):
    return crud_estado.get_distinct_anios(db)


@router.get("/obtener-por-anio/{anio}", response_model=List[RetornoEstadoNorma])
def obtener_por_anio(anio: int, db: Session = Depends(get_db)):
    r = crud_estado.get_estado_by_anio(db, anio)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para este año")
    return r


@router.get("/valores-vigencia", response_model=List[str])
def valores_vigencia(db: Session = Depends(get_db)):
    return crud_estado.get_distinct_vigencias(db)


@router.get("/obtener-por-vigencia/{vigencia}", response_model=List[RetornoEstadoNorma])
def obtener_por_vigencia(vigencia: str, db: Session = Depends(get_db)):
    r = crud_estado.get_estado_by_vigencia(db, vigencia)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para esta vigencia")
    return r


@router.get("/valores-tipo_norma", response_model=List[str])
def valores_tipo_norma(db: Session = Depends(get_db)):
    return crud_estado.get_distinct_tipo_norma(db)


@router.get("/obtener-por-tipo_norma/{tipo_norma}", response_model=List[RetornoEstadoNorma])
def obtener_por_tipo_norma(tipo_norma: str, db: Session = Depends(get_db)):
    r = crud_estado.get_estado_by_tipo_norma(db, tipo_norma)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para este tipo_norma")
    return r


@router.get("/valores-mesa_sectorial", response_model=List[str])
def valores_mesa_sectorial(db: Session = Depends(get_db)):
    return crud_estado.get_distinct_mesa_sectorial(db)


@router.get("/obtener-por-mesa_sectorial/{mesa}", response_model=List[RetornoEstadoNorma])
def obtener_por_mesa_sectorial(mesa: str, db: Session = Depends(get_db)):
    r = crud_estado.get_estado_by_mesa_sectorial(db, mesa)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para esta mesa_sectorial")
    return r
