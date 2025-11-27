from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Optional, List

from app.schemas.estado_normas import CrearEstadoNorma, EditarEstadoNorma

logger = logging.getLogger(__name__)

def crear_estado_norma(db: Session, norma: CrearEstadoNorma) -> bool:
    try:
        data = norma.model_dump()
        query = text("""
            INSERT INTO estado_de_normas (
                cod_programa, cod_version, fecha_elaboracion, anio, red_conocimiento,
                nombre_ncl, cod_ncl, ncl_version, norma_corte_noviembre,
                version, norma_version, mesa_sectorial, tipo_norma,
                observacion, fecha_revision, tipo_competencia, vigencia, fecha_indice
            ) VALUES (
                :cod_programa, :cod_version, :fecha_elaboracion, :anio, :red_conocimiento,
                :nombre_ncl, :cod_ncl, :ncl_version, :norma_corte_noviembre,
                :version, :norma_version, :mesa_sectorial, :tipo_norma,
                :observacion, :fecha_revision, :tipo_competencia, :vigencia, :fecha_indice
            )
        """)
        db.execute(query, data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error crear_estado_norma: {e}")
        raise Exception("Error al crear registro en estado_de_normas")

def listar_normas(db: Session):
    try:
        query = text("SELECT * FROM estado_de_normas ORDER BY id_estado_norma ASC")
        return db.execute(query).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error listar_normas: {e}")
        raise Exception("Error al listar normas")

def obtener_norma_por_id(db: Session, id_norma: int):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE id_estado_norma = :id")
        return db.execute(query, {"id": id_norma}).mappings().first()
    except SQLAlchemyError as e:
        logger.error(f"Error obtener_norma_por_id: {e}")
        raise Exception("Error al obtener norma")

def listar_normas_por_programa(db: Session, cod_programa: int):
    try:
        query = text("SELECT * FROM estado_de_normas WHERE cod_programa = :cod_programa ORDER BY id_estado_norma ASC")
        return db.execute(query, {"cod_programa": cod_programa}).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error listar_normas_por_programa: {e}")
        raise Exception("Error al listar normas por programa")

def actualizar_norma(db: Session, id_norma: int, data_update: EditarEstadoNorma) -> bool:
    try:
        fields = data_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{k} = :{k}" for k in fields])
        fields["id"] = id_norma
        query = text(f"UPDATE estado_de_normas SET {set_clause} WHERE id_estado_norma = :id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error actualizar_norma: {e}")
        raise Exception("Error al actualizar norma")

def eliminar_norma(db: Session, id_norma: int) -> bool:
    try:
        query = text("DELETE FROM estado_de_normas WHERE id_estado_norma = :id")
        db.execute(query, {"id": id_norma})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error eliminar_norma: {e}")
        raise Exception("Error al eliminar norma")
