from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.schemas.registro_calificado import RetornoRegistroCalificado
from app.crud import registro_calificado as crud_registro
from core.database import get_db

router = APIRouter(prefix="/registro_calificado", tags=["Registro Calificado"])


@router.get("/listar", response_model=List[RetornoRegistroCalificado])
def listar(db: Session = Depends(get_db)):
    return crud_registro.listar_registros(db)


# Nota: se usa un endpoint explícito `/obtener-por-cod_programa/{cod_programa}`
# para evitar que un path dinámico genérico capture rutas estáticas como
# `/valores-clasificacion`. El endpoint genérico `/{cod_programa}` se eliminó
# deliberadamente para evitar conflictos de enrutamiento.


@router.get("/obtener-por-cod_programa/{cod_programa}", response_model=RetornoRegistroCalificado)
def obtener_por_cod_programa(cod_programa: str, db: Session = Depends(get_db)):
    r = crud_registro.obtener_registro_por_id(db, cod_programa)
    if not r:
        raise HTTPException(status_code=404, detail="Registro calificado no encontrado")
    return r


# Endpoints de consulta por campos y valores únicos (para selects)

@router.get("/obtener-por-modalidad/{modalidad}", response_model=List[RetornoRegistroCalificado])
def obtener_por_modalidad(modalidad: str, db: Session = Depends(get_db)):
    r = crud_registro.get_registros_by_modalidad(db, modalidad)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para esta modalidad")
    return r


@router.get("/valores-modalidad", response_model=List[str])
def valores_modalidad(db: Session = Depends(get_db)):
    return crud_registro.get_distinct_modalidades(db)


@router.get("/obtener-por-clasificacion/{clasificacion}", response_model=List[RetornoRegistroCalificado])
def obtener_por_clasificacion(clasificacion: str, db: Session = Depends(get_db)):
    r = crud_registro.get_registros_by_clasificacion(db, clasificacion)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para esta clasificación")
    return r


@router.get("/valores-clasificacion", response_model=List[str])
def valores_clasificacion(db: Session = Depends(get_db)):
    return crud_registro.get_distinct_clasificaciones(db)


@router.get("/obtener-por-vigencia/{vigencia}", response_model=List[RetornoRegistroCalificado])
def obtener_por_vigencia(vigencia: str, db: Session = Depends(get_db)):
    r = crud_registro.get_registros_by_vigencia(db, vigencia)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para esta vigencia")
    return r


@router.get("/valores-vigencia", response_model=List[str])
def valores_vigencia(db: Session = Depends(get_db)):
    return crud_registro.get_distinct_vigencias(db)


@router.get("/obtener-por-estado_catalogo/{estado_catalogo}", response_model=List[RetornoRegistroCalificado])
def obtener_por_estado_catalogo(estado_catalogo: str, db: Session = Depends(get_db)):
    r = crud_registro.get_registros_by_estado_catalogo(db, estado_catalogo)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para este estado_catalogo")
    return r


@router.get("/valores-estado_catalogo", response_model=List[str])
def valores_estado_catalogo(db: Session = Depends(get_db)):
    return crud_registro.get_distinct_estado_catalogo(db)


@router.get("/obtener-por-tipo_tramite/{tipo_tramite}", response_model=List[RetornoRegistroCalificado])
def obtener_por_tipo_tramite(tipo_tramite: str, db: Session = Depends(get_db)):
    r = crud_registro.get_registros_by_tipo_tramite(db, tipo_tramite)
    if not r:
        raise HTTPException(status_code=404, detail="No se encontraron registros para este tipo_tramite")
    return r


@router.get("/valores-tipo_tramite", response_model=List[str])
def valores_tipo_tramite(db: Session = Depends(get_db)):
    return crud_registro.get_distinct_tipo_tramite(db)
