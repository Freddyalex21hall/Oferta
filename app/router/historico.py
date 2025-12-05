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
    (DIURNO, NOCTURNO, MIXTO)
    La jornada corresponde a la jornada en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_jornada(db, jornada)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para esta jornada")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/obtener-por-estado-curso/{estado_curso}", status_code=status.HTTP_200_OK)
def get_by_estado_curso(
    estado_curso: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un estado_curso específico.
    (Terminada, Terminada por fecha)
    La estado_curso corresponde al estado_curso en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_estado_curso(db, estado_curso)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este estado_curso")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/obtener-por-fecha_inicio/{fecha_inicio}", status_code=status.HTTP_200_OK)
def get_by_fecha_inicio(
    fecha_inicio: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un estado_curso específico.
    (Terminada, Terminada por fecha) Fecha en formato YYYY-MM-DD
    La estado_curso corresponde al estado_curso en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_fecha_inicio(db, fecha_inicio)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para esta fecha_inicio")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS ====================

@router.get("/obtener-por-fecha_inicio/{fecha_inicio}", status_code=status.HTTP_200_OK)
def get_by_fecha_inicio(
    fecha_inicio: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a una fecha_inicio específica.
    Fecha en formato YYYY-MM-DD
    La fecha_inicio corresponde a la fecha_inicio en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_fecha_inicio(db, fecha_inicio)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para esta fecha_inicio")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-fecha_fin/{fecha_fin}", status_code=status.HTTP_200_OK)
def get_by_fecha_fin(
    fecha_fin: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a una fecha_fin específica.
    Fecha en formato YYYY-MM-DD
    La fecha_fin corresponde a la fecha_fin en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_fecha_fin(db, fecha_fin)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para esta fecha_fin")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-cod_municipio/{cod_municipio}", status_code=status.HTTP_200_OK)
def get_by_cod_municipio(
    cod_municipio: str,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un cod_municipio específico.
    El cod_municipio corresponde al cod_municipio en la tabla grupos.
    """
    try:
        historico = crud_historico.get_historico_by_cod_municipio(db, cod_municipio)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este cod_municipio")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_inscritos/{num_aprendices_inscritos}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_inscritos(
    num_aprendices_inscritos: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_inscritos específico.
    El num_aprendices_inscritos corresponde al num_aprendices_inscritos en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_inscritos(db, num_aprendices_inscritos)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_inscritos")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_en_transito/{num_aprendices_en_transito}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_en_transito(
    num_aprendices_en_transito: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_en_transito específico.
    El num_aprendices_en_transito corresponde al num_aprendices_en_transito en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_en_transito(db, num_aprendices_en_transito)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_en_transito")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_formacion/{num_aprendices_formacion}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_formacion(
    num_aprendices_formacion: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_formacion específico.
    El num_aprendices_formacion corresponde al num_aprendices_formacion en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_formacion(db, num_aprendices_formacion)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_formacion")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_induccion/{num_aprendices_induccion}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_induccion(
    num_aprendices_induccion: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_induccion específico.
    El num_aprendices_induccion corresponde al num_aprendices_induccion en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_induccion(db, num_aprendices_induccion)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_induccion")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_condicionados/{num_aprendices_condicionados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_condicionados(
    num_aprendices_condicionados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_condicionados específico.
    El num_aprendices_condicionados corresponde al num_aprendices_condicionados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_condicionados(db, num_aprendices_condicionados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_condicionados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_aplazados/{num_aprendices_aplazados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_aplazados(
    num_aprendices_aplazados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_aplazados específico.
    El num_aprendices_aplazados corresponde al num_aprendices_aplazados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_aplazados(db, num_aprendices_aplazados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_aplazados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_retirado_voluntario/{num_aprendices_retirado_voluntario}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_retirado_voluntario(
    num_aprendices_retirado_voluntario: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_retirado_voluntario específico.
    El num_aprendices_retirado_voluntario corresponde al num_aprendices_retirado_voluntario en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_retirado_voluntario(db, num_aprendices_retirado_voluntario)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_retirado_voluntario")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_cancelados/{num_aprendices_cancelados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_cancelados(
    num_aprendices_cancelados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_cancelados específico.
    El num_aprendices_cancelados corresponde al num_aprendices_cancelados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_cancelados(db, num_aprendices_cancelados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_cancelados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_reprobados/{num_aprendices_reprobados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_reprobados(
    num_aprendices_reprobados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_reprobados específico.
    El num_aprendices_reprobados corresponde al num_aprendices_reprobados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_reprobados(db, num_aprendices_reprobados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_reprobados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_no_aptos/{num_aprendices_no_aptos}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_no_aptos(
    num_aprendices_no_aptos: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_no_aptos específico.
    El num_aprendices_no_aptos corresponde al num_aprendices_no_aptos en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_no_aptos(db, num_aprendices_no_aptos)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_no_aptos")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_reingresados/{num_aprendices_reingresados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_reingresados(
    num_aprendices_reingresados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_reingresados específico.
    El num_aprendices_reingresados corresponde al num_aprendices_reingresados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_reingresados(db, num_aprendices_reingresados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_reingresados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_por_certificar/{num_aprendices_por_certificar}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_por_certificar(
    num_aprendices_por_certificar: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_por_certificar específico.
    El num_aprendices_por_certificar corresponde al num_aprendices_por_certificar en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_por_certificar(db, num_aprendices_por_certificar)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_por_certificar")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/obtener-por-num_aprendices_certificados/{num_aprendices_certificados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_certificados(
    num_aprendices_certificados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_certificados específico.
    El num_aprendices_certificados corresponde al num_aprendices_certificados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_certificados(db, num_aprendices_certificados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_certificados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/obtener-por-num_aprendices_trasladados/{num_aprendices_trasladados}", status_code=status.HTTP_200_OK)
def get_by_num_aprendices_trasladados(
    num_aprendices_trasladados: int,
    db: Session = Depends(get_db),
    user_token: RetornoUsuario = Depends(get_current_user)
):
    """
    Obtiene el histórico asociado a un num_aprendices_trasladados específico.
    El num_aprendices_trasladados corresponde al num_aprendices_trasladados en la tabla historico.
    """
    try:
        historico = crud_historico.get_historico_by_num_aprendices_trasladados(db, num_aprendices_trasladados)
        if historico is None:
            raise HTTPException(status_code=404, detail="No se encontró histórico para este num_aprendices_trasladados")
        return historico
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))