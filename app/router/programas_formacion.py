from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.schemas.programas_formacion import CrearPrograma, EditarPrograma, RetornoPrograma
from app.crud.programas_formacion import (
    crear_programa, listar_programas, obtener_programa_por_id,
    actualizar_programa, eliminar_programa
)
from core.database import get_db

router = APIRouter(prefix="/programas_formacion", tags=["Programas Formación"])

@router.post("/crear", status_code=status.HTTP_201_CREATED)
def crear(p: CrearPrograma, db: Session = Depends(get_db)):
    if crear_programa(db, p):
        return {"message": "Programa creado correctamente"}
    raise HTTPException(status_code=500, detail="Error al crear programa")

@router.get("/listar", response_model=List[RetornoPrograma])
def listar(db: Session = Depends(get_db)):
    return listar_programas(db)

@router.get("/{cod_programa}", response_model=RetornoPrograma)
def obtener(cod_programa: int, db: Session = Depends(get_db)):
    r = obtener_programa_por_id(db, cod_programa)
    if not r:
        raise HTTPException(status_code=404, detail="Programa no encontrado")
    return r

@router.put("/actualizar/{cod_programa}")
def actualizar(cod_programa: int, data: EditarPrograma, db: Session = Depends(get_db)):
    if actualizar_programa(db, cod_programa, data):
        return {"message": "Programa actualizado correctamente"}
    raise HTTPException(status_code=400, detail="No hay datos para actualizar")

@router.delete("/eliminar/{cod_programa}")
def eliminar(cod_programa: int, db: Session = Depends(get_db)):
    if eliminar_programa(db, cod_programa):
        return {"message": "Programa eliminado correctamente"}
    raise HTTPException(status_code=500, detail="Error al eliminar programa")
