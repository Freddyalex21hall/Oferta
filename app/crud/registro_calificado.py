from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

from app.schemas.registro_calificado import CrearRegistroCalificado, EditarRegistroCalificado

logger = logging.getLogger(__name__)


def crear_registro(db: Session, registro: CrearRegistroCalificado) -> bool:
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
        logger.error(f"Error crear_registro: {e}")
        raise Exception("Error de base de datos al crear registro calificado")


def listar_registros(db: Session):
    try:
        query = text("SELECT * FROM registro_calificado ORDER BY cod_programa ASC")
        return db.execute(query).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Error listar_registros: {e}")
        raise Exception("Error de base de datos al listar registros calificados")


def obtener_registro_por_id(db: Session, cod_programa: str):
    try:
        query = text("SELECT * FROM registro_calificado WHERE cod_programa = :id")
        return db.execute(query, {"id": cod_programa}).mappings().first()
    except SQLAlchemyError as e:
        logger.error(f"Error obtener_registro_por_id: {e}")
        raise Exception("Error de base de datos al obtener registro calificado")


def actualizar_registro(db: Session, cod_programa: str, data_update: EditarRegistroCalificado) -> bool:
    try:
        fields = data_update.model_dump(exclude_unset=True)
        if not fields:
            return False
        set_clause = ", ".join([f"{k} = :{k}" for k in fields])
        fields["id"] = cod_programa
        query = text(f"UPDATE registro_calificado SET {set_clause} WHERE cod_programa = :id")
        db.execute(query, fields)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error actualizar_registro: {e}")
        raise Exception("Error de base de datos al actualizar registro calificado")


def eliminar_registro(db: Session, cod_programa: str) -> bool:
    try:
        query = text("DELETE FROM registro_calificado WHERE cod_programa = :id")
        db.execute(query, {"id": cod_programa})
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error eliminar_registro: {e}")
        raise Exception("Error de base de datos al eliminar registro calificado")
