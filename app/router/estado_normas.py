from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from core.database import get_db
from app.schemas.estado_normas import CrearEstadoNorma, EditarEstadoNorma
from app.crud.estado_normas import (
    crear_estado_norma,
    listar_estado_normas,
    obtener_estado_norma,
    actualizar_estado_norma,
    eliminar_estado_norma,
)

router = APIRouter()


# 1. Crear un registro
@router.post("/crear", status_code=status.HTTP_201_CREATED)
def crear(data: CrearEstadoNorma, db: Session = Depends(get_db)):
    return crear_estado_norma(db, data.dict())


# 2. Listar todos
@router.get("/listar")
def listar(db: Session = Depends(get_db)):
    return listar_estado_normas(db)


# 3. Obtener por ID
@router.get("/obtener/{id}")
def obtener(id: int, db: Session = Depends(get_db)):
    norma = obtener_estado_norma(db, id)
    if not norma:
        raise HTTPException(status_code=404, detail="Estado de norma no encontrado")
    return norma


# 4. Actualizar
@router.put("/actualizar/{id}")
def actualizar(id: int, data: EditarEstadoNorma, db: Session = Depends(get_db)):
    actualizado = actualizar_estado_norma(db, id, data.dict(exclude_unset=True))

    if not actualizado:
        raise HTTPException(status_code=404, detail="Estado de norma no encontrado")

    return {"mensaje": "Actualizado correctamente"}


# 5. Eliminar
@router.delete("/eliminar/{id}", status_code=status.HTTP_204_NO_CONTENT)
def eliminar(id: int, db: Session = Depends(get_db)):
    eliminado = eliminar_estado_norma(db, id)
    if not eliminado:
        raise HTTPException(status_code=404, detail="Estado de norma no encontrado")

    return {"mensaje": "Eliminado correctamente"}
