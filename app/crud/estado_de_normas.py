from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
import logging

from app.schemas.estado_de_normas import CrearEstado, EditarEstado, RetornoEstado

logger = logging.getLogger(__name__)


def create_estado(db: Session, estado: CrearEstado) -> Optional[bool]:
    try:
        data = estado.model_dump()

        query = text("""
            INSERT INTO estado_de_normas (
                cod_version, fecha_elaboracion, anio, red_conocimiento,
                nombre_ncl, cod_ncl, ncl_version, norma_corte_noviembre,
                verion, norma_version, mesa_sectorial, tipo_norma,
                observacion, fecha_revision, tipo_competencia, vigencia,
                fecha_indice
            ) VALUES (
                :cod_version, :fecha_elaboracion, :anio, :red_conocimiento,
                :nombre_ncl, :cod_ncl, :ncl_version, :norma_corte_noviembre,
                :verion, :norma_version, :mesa_sectorial, :tipo_norma,
                :observacion, :fecha_revision, :tipo_competencia, :vigencia,
                :fecha_indice
            )
        """)

        db.execute(query, data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear estado_de_normas: {e}")
        raise Exception("Error de base de datos al crear el estado de normas")


def get_estado_by_id(db: Session, cod_programa: int):
    try:
        query = text("""
            SELECT *
            FROM estado_de_normas
            WHERE cod_programa = :cod_programa
        """)

        result = db.execute(query, {"cod_programa": cod_programa}).mappings().first()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al buscar estado_de_normas por id: {e}")
        raise Exception("Error de base de datos al buscar el estado de normas")


def get_all_estados(db: Session, skip: int = 0, limit: int = 100) -> List[dict]:
    try:
        query = text("""
            SELECT *
            FROM estado_de_normas
            LIMIT :limit OFFSET :skip
        """)

        result = db.execute(query, {"limit": limit, "skip": skip}).mappings().all()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener estados: {e}")
        raise Exception("Error de base de datos al obtener los estados")


def get_estados_by_anio(db: Session, anio: int) -> List[dict]:
    try:
        query = text("""
            SELECT *
            FROM estado_de_normas
            WHERE anio = :anio
        """)

        result = db.execute(query, {"anio": anio}).mappings().all()
        return result

    except SQLAlchemyError as e:
        logger.error(f"Error al obtener estados por año: {e}")
        raise Exception("Error de base de datos al obtener los estados por año")


def estado_delete(db: Session, cod_programa: int) -> bool:
    try:
        query = text("""
            DELETE FROM estado_de_normas
            WHERE cod_programa = :cod_programa
        """)

        db.execute(query, {"cod_programa": cod_programa})
        db.commit()

        return True

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar estado_de_normas por id: {e}")
        raise Exception("Error de base de datos al eliminar el estado de normas")


def update_estado(db: Session, cod_programa: int, estado_update: EditarEstado) -> bool:
    try:
        fields = estado_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{key} = :{key}" for key in fields])
        fields["cod_programa"] = cod_programa

        query = text(f"UPDATE estado_de_normas SET {set_clause} WHERE cod_programa = :cod_programa")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar estado_de_normas: {e}")
        raise Exception("Error de base de datos al actualizar el estado de normas")
