from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.schemas.registro_calificado import (
    CrearRegistroCalificado, EditarRegistroCalificado, RetornoRegistroCalificado
)
from app.crud import registro_calificado as crud_registro
from core.database import get_db

router = APIRouter(prefix="/registro_calificado", tags=["Registro Calificado"])


@router.post("/crear", status_code=status.HTTP_201_CREATED)
def crear(reg: CrearRegistroCalificado, db: Session = Depends(get_db)):
    if crud_registro.crear_registro(db, reg):
        return {"message": "Registro calificado creado correctamente"}
    raise HTTPException(status_code=500, detail="Error al crear registro calificado")


@router.get("/listar", response_model=List[RetornoRegistroCalificado])
def listar(db: Session = Depends(get_db)):
    return crud_registro.listar_registros(db)


@router.get("/{cod_programa}", response_model=RetornoRegistroCalificado)
def obtener(cod_programa: str, db: Session = Depends(get_db)):
    r = crud_registro.obtener_registro_por_id(db, cod_programa)
    if not r:
        raise HTTPException(status_code=404, detail="Registro calificado no encontrado")
    return r


@router.put("/actualizar/{cod_programa}")
def actualizar(cod_programa: str, data: EditarRegistroCalificado, db: Session = Depends(get_db)):
    if crud_registro.actualizar_registro(db, cod_programa, data):
        return {"message": "Registro calificado actualizado correctamente"}
    raise HTTPException(status_code=400, detail="No hay datos para actualizar")


@router.delete("/eliminar/{cod_programa}")
def eliminar(cod_programa: str, db: Session = Depends(get_db)):
    if crud_registro.eliminar_registro(db, cod_programa):
        return {"message": "Registro calificado eliminado correctamente"}
    raise HTTPException(status_code=500, detail="Error al eliminar registro calificado")
