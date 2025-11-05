from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import List

from app.schemas.estado_de_normas import CrearEstado, EditarEstado, RetornoEstado
from core.database import get_db
from app.crud import estado_de_normas as crud_estado

router = APIRouter()


@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_estado(estado: CrearEstado, db: Session = Depends(get_db)):
    try:
        crear = crud_estado.create_estado(db, estado)
        if crear:
            return {"message": "Estado de normas creado correctamente"}
        else:
            return {"message": "No se pudo crear el estado de normas"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-id/{cod_programa}", status_code=status.HTTP_200_OK)
def get_by_id(cod_programa: int, db: Session = Depends(get_db)):
    try:
        estado = crud_estado.get_estado_by_id(db, cod_programa)
        if estado is None:
            raise HTTPException(status_code=404, detail="Estado no encontrado")
        return estado
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-todos", status_code=status.HTTP_200_OK)
def get_all(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    try:
        estados = crud_estado.get_all_estados(db, skip=skip, limit=limit)
        return estados
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-anio/{anio}", status_code=status.HTTP_200_OK)
def get_by_anio(anio: int, db: Session = Depends(get_db)):
    try:
        estados = crud_estado.get_estados_by_anio(db, anio)
        if not estados:
            raise HTTPException(status_code=404, detail="No se encontraron registros para este año")
        return estados
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/eliminar-por-id/{cod_programa}", status_code=status.HTTP_200_OK)
def delete_by_id(cod_programa: int, db: Session = Depends(get_db)):
    try:
        resultado = crud_estado.estado_delete(db, cod_programa)
        if resultado:
            return {"message": "Estado eliminado correctamente"}
        else:
            raise HTTPException(status_code=404, detail="Estado no encontrado")
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/editar/{cod_programa}", status_code=status.HTTP_200_OK)
def update_estado(cod_programa: int, estado: EditarEstado, db: Session = Depends(get_db)):
    try:
        success = crud_estado.update_estado(db, cod_programa, estado)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el estado")
        return {"message": "Estado actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
