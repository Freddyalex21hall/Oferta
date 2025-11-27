from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.schemas.registro_calificado import (
    CrearRegistroCalificado,
    EditarRegistroCalificado,
    RetornoRegistroCalificado,
)
from core.database import get_db
from app.crud import registro_calificado as crud_registro
from app.router.dependencies import get_current_user
from app.schemas.usuarios import RetornoUsuario

router = APIRouter()


@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_registro(
    registro: CrearRegistroCalificado,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user),
):
    try:
        creado = crud_registro.create_registro(db, registro)
        if creado:
            return {"message": "Registro calificado creado correctamente"}
        return {"message": "No se pudo crear el registro calificado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-cod/{cod_programa}", status_code=status.HTTP_200_OK)
def get_by_cod(
    cod_programa: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user),
):
    try:
        registro = crud_registro.get_registro_by_cod(db, cod_programa)
        if registro is None:
            raise HTTPException(status_code=404, detail="Registro no encontrado")
        return registro
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-todos", status_code=status.HTTP_200_OK)
def get_all(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user),
):
    try:
        registros = crud_registro.get_all_registros(db, skip=skip, limit=limit)
        return registros
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/editar/{cod_programa}", status_code=status.HTTP_200_OK)
def update_registro(
    cod_programa: int,
    registro: EditarRegistroCalificado,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user),
):
    try:
        success = crud_registro.update_registro(db, cod_programa, registro)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el registro")
        return {"message": "Registro actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/eliminar/{cod_programa}", status_code=status.HTTP_200_OK)
def delete_registro(
    cod_programa: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user),
):
    try:
        resultado = crud_registro.delete_registro(db, cod_programa)
        if resultado:
            return {"message": "Registro eliminado correctamente"}
        raise HTTPException(status_code=404, detail="Registro no encontrado")
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
