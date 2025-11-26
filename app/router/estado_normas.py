from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from app.schemas.estado_normas import CrearEstadoNorma, EditarEstadoNorma, RetornoEstadoNorma
from app.crud.estado_normas import (
    crear_estado_norma, listar_normas, obtener_norma_por_id,
    listar_normas_por_programa, actualizar_norma, eliminar_norma
)
from core.database import get_db

router = APIRouter(prefix="/estado_normas", tags=["Estado Normas"])

@router.post("/crear", status_code=status.HTTP_201_CREATED)
def crear(norma: CrearEstadoNorma, db: Session = Depends(get_db)):
    # Opcional: validar que existe el programa antes de insertar
    if crear_estado_norma(db, norma):
        return {"message": "Norma creada correctamente"}
    raise HTTPException(status_code=500, detail="Error al crear norma")

@router.get("/listar", response_model=List[RetornoEstadoNorma])
def listar(db: Session = Depends(get_db)):
    return listar_normas(db)

@router.get("/{id_norma}", response_model=RetornoEstadoNorma)
def obtener(id_norma: int, db: Session = Depends(get_db)):
    r = obtener_norma_por_id(db, id_norma)
    if not r:
        raise HTTPException(status_code=404, detail="Norma no encontrada")
    return r

@router.get("/programa/{cod_programa}", response_model=List[RetornoEstadoNorma])
def listar_por_programa(cod_programa: int, db: Session = Depends(get_db)):
    return listar_normas_por_programa(db, cod_programa)

@router.put("/actualizar/{id_norma}")
def actualizar(id_norma: int, data: EditarEstadoNorma, db: Session = Depends(get_db)):
    if actualizar_norma(db, id_norma, data):
        return {"message": "Norma actualizada correctamente"}
    raise HTTPException(status_code=400, detail="No hay datos para actualizar")

@router.delete("/eliminar/{id_norma}")
def eliminar(id_norma: int, db: Session = Depends(get_db)):
    if eliminar_norma(db, id_norma):
        return {"message": "Norma eliminada correctamente"}
    raise HTTPException(status_code=500, detail="Error al eliminar norma")
