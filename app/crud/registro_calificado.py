from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List
import logging

from app.schemas.registro_calificado import (
    CrearRegistroCalificado,
    EditarRegistroCalificado,
    RetornoRegistroCalificado,
)

logger = logging.getLogger(__name__)


def create_registro(db: Session, registro: CrearRegistroCalificado) -> Optional[bool]:
    try:
        data = registro.model_dump()

        query = text("""
            INSERT INTO registro_calificado (
                cod_programa, tipo_tramite, fecha_radicado,
                numero_resolucion, fecha_resolucion, fecha_vencimiento,
                vigencia, modalidad, clasificacion, estado_catalogo
            ) VALUES (
                :cod_programa, :tipo_tramite, :fecha_radicado,
                :numero_resolucion, :fecha_resolucion, :fecha_vencimiento,
                :vigencia, :modalidad, :clasificacion, :estado_catalogo
            )
        """)
        db.execute(query, data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear registro_calificado: {e}")
        raise Exception("Error de base de datos al crear el registro calificado")


def get_registro_by_cod(db: Session, cod_programa: int) -> Optional[dict]:
    try:
        query = text("""
            SELECT * FROM registro_calificado
            WHERE cod_programa = :cod_programa
        """)
        result = db.execute(query, {"cod_programa": cod_programa}).mappings().first()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener registro por cod: {e}")
        raise Exception("Error de base de datos al obtener el registro calificado")


def get_all_registros(db: Session, skip: int = 0, limit: int = 100) -> List[dict]:
    try:
        query = text("""
            SELECT * FROM registro_calificado
            LIMIT :limit OFFSET :skip
        """)
        result = db.execute(query, {"limit": limit, "skip": skip}).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener registros: {e}")
        raise Exception("Error de base de datos al obtener los registros calificados")


def update_registro(db: Session, cod_programa: int, registro_update: EditarRegistroCalificado) -> bool:
    try:
        fields = registro_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{key} = :{key}" for key in fields])
        fields["cod_programa"] = cod_programa

        query = text(f"UPDATE registro_calificado SET {set_clause} WHERE cod_programa = :cod_programa")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar registro: {e}")
        raise Exception("Error de base de datos al actualizar el registro calificado")


def delete_registro(db: Session, cod_programa: int) -> bool:
    try:
        query = text("""
            DELETE FROM registro_calificado
            WHERE cod_programa = :cod_programa
        """)
        db.execute(query, {"cod_programa": cod_programa})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar registro: {e}")
        raise Exception("Error de base de datos al eliminar el registro calificado")
