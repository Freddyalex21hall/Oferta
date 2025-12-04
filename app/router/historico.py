from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import List

from app.schemas.historico import RetornoHistorico
from core.database import get_db
from app.crud import historico as crud_historico
from app.router.dependencies import get_current_user
from app.schemas.usuarios import RetornoUsuario

router = APIRouter()

@router.get("/obtener-todos", status_code=status.HTTP_200_OK)
def get_all(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        historicos = crud_historico.get_all_historicos(db, skip=skip, limit=limit)
        return historicos
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/obtener-por-id/{id_historico}", status_code=status.HTTP_200_OK)
def get_by_id(
    id_historico: int, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        historico = crud_historico.get_historico_by_id(db, id_historico)
        if historico is None:
            raise HTTPException(status_code=404, detail="Historico no encontrado")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))




@router.get("/obtener-por-grupo/{id_grupo}", status_code=status.HTTP_200_OK)
def get_by_grupo(
    id_grupo: int, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    try:
        historicos = crud_historico.get_historicos_by_grupo(db, id_grupo)
        if not historicos:
            raise HTTPException(status_code=404, detail="No se encontraron historicos para este grupo")
        return historicos
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-ficha/{ficha}", status_code=status.HTTP_200_OK)
def get_by_ficha(
    ficha: int, 
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a una ficha específica.
    La ficha es única y corresponde al id_grupo en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_ficha(db, ficha)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para esta ficha")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-cod_programa/{cod_programa}", status_code=status.HTTP_200_OK)
def get_by_cod_programa(
    cod_programa: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un codigo de programa específico.
    El codigo de programa es único y corresponde al cod_programa en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_cod_programa(db, cod_programa)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este codigo de programa")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/obtener-por-cod_centro/{cod_centro}", status_code=status.HTTP_200_OK)
def get_by_cod_centro(
    cod_centro: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un codigo de programa específico.
    El codigo de centro es único y corresponde al cod_centro en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_cod_centro(db, cod_centro)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este codigo de centro")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/obtener-por-jornada/{jornada}", status_code=status.HTTP_200_OK)
def get_by_jornada(
    jornada: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un jornada específico.
    La jornada corresponde a la jornada en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_jornada(db, jornada)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para esta jornada")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

