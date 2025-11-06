from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Optional

from app.schemas.programas_formacion import CrearPrograma, EditarPrograma

logger = logging.getLogger(__name__)

def crear_programa(db: Session, programa: CrearPrograma) -> bool:
    try:
        data = programa.model_dump()
        query = text("""
            INSERT INTO programas_formacion (
                version, nombre, nivel, meses_duracion,
                duracion_programa, unidad_medida, estado,
                tipo_programa, url_pdf, red_conocimiento, programa_especial
            ) VALUES (
                :version, :nombre, :nivel, :meses_duracion,
                :duracion_programa, :unidad_medida, :estado,
                :tipo_programa, :url_pdf, :red_conocimiento, :programa_especial
            )
        """)
        db.execute(query, data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error crear_programa: {e}")
        raise Exception("Error de base de datos al crear programa")

def listar_programas(db: Session):
    try:
        query = text("SELECT * FROM programas_formacion ORDER BY cod_programa ASC")
        return db.execute(query).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error listar_programas: {e}")
        raise Exception("Error de base de datos al listar programas")

def obtener_programa_por_id(db: Session, cod_programa: int):
    try:
        query = text("SELECT * FROM programas_formacion WHERE cod_programa = :id")
        return db.execute(query, {"id": cod_programa}).mappings().first()
    except SQLAlchemyError as e:
        logger.error(f"Error obtener_programa_por_id: {e}")
        raise Exception("Error de base de datos al obtener programa")

def actualizar_programa(db: Session, cod_programa: int, data_update: EditarPrograma) -> bool:
    try:
        fields = data_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{k} = :{k}" for k in fields])
        fields["id"] = cod_programa
        query = text(f"UPDATE programas_formacion SET {set_clause} WHERE cod_programa = :id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error actualizar_programa: {e}")
        raise Exception("Error de base de datos al actualizar programa")

def eliminar_programa(db: Session, cod_programa: int) -> bool:
    try:
        query = text("DELETE FROM programas_formacion WHERE cod_programa = :id")
        db.execute(query, {"id": cod_programa})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error eliminar_programa: {e}")
        raise Exception("Error de base de datos al eliminar programa")
