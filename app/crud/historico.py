from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
import logging

from app.schemas.historico import EditarHistorico, RetornoHistorico

logger = logging.getLogger(__name__)


def get_historico_by_id(db: Session, id_historico: int):
    try:
        query = text("""
            SELECT historico.id_historico, historico.id_grupo, 
                    historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                    historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                    historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                    historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                    historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                    historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                    historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            WHERE historico.id_historico = :id_historico
        """)

        result = db.execute(query, {"id_historico": id_historico}).mappings().first()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al buscar historico por id: {e}")
        raise Exception("Error de base de datos al buscar el historico")


def get_all_historicos(db: Session, skip: int = 0, limit: int = 100) -> List[dict]:
    try:
        query = text("""
            SELECT historico.id_historico, historico.id_grupo, 
                    historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                    historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                    historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                    historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                    historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                    historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                    historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            LIMIT :limit OFFSET :skip
        """)

        result = db.execute(query, {"limit": limit, "skip": skip}).mappings().all()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historicos: {e}")
        raise Exception("Error de base de datos al obtener los historicos")


def get_historicos_by_grupo(db: Session, id_grupo: int) -> List[dict]:
    try:
        query = text("""
            SELECT historico.id_historico, historico.id_grupo, 
                    historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                    historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                    historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                    historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                    historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                    historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                    historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            WHERE historico.id_grupo = :id_grupo
        """)

        result = db.execute(query, {"id_grupo": id_grupo}).mappings().all()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historicos por grupo: {e}")
        raise Exception("Error de base de datos al obtener los historicos por grupo")


def get_historico_by_ficha(db: Session, ficha: int) -> Optional[dict]:
    """
    Obtiene el histórico asociado a una ficha específica.
    La ficha es única y corresponde al id_grupo en la tabla historico.
    """
    try:
        query = text("""
            SELECT historico.id_historico, historico.id_grupo, 
                    historico.num_aprendices_inscritos, historico.num_aprendices_en_transito,
                    historico.num_aprendices_formacion, historico.num_aprendices_induccion,
                    historico.num_aprendices_condicionados, historico.num_aprendices_aplazados,
                    historico.num_aprendices_retirado_voluntario, historico.num_aprendices_cancelados,
                    historico.num_aprendices_reprobados, historico.num_aprendices_no_aptos,
                    historico.num_aprendices_reingresados, historico.num_aprendices_por_certificar,
                    historico.num_aprendices_certificados, historico.num_aprendices_trasladados
            FROM historico
            WHERE historico.id_grupo = :ficha
        """)

        result = db.execute(query, {"ficha": ficha}).mappings().first()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener historico por ficha: {e}")
        raise Exception("Error de base de datos al obtener el historico por ficha")


def update_historico(db: Session, historico_id: int, historico_update: EditarHistorico) -> bool:
    try:
        fields = historico_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{key} = :{key}" for key in fields])
        fields["historico_id"] = historico_id

        query = text(f"UPDATE historico SET {set_clause} WHERE id_historico = :historico_id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar historico: {e}")
        raise Exception("Error de base de datos al actualizar el historico")
